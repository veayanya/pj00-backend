// lib/db.js
// Adapter penyimpanan untuk backend Express (Render).
//
// v3 — sekarang berjalan di atas lib/dbPool.js:
// • Mendukung hingga 1000 database Neon PostgreSQL sekaligus.
// • Jika database aktif menyentuh limit/kuota, data otomatis dicadangkan
// lalu dipindahkan (backup + restore + verifikasi) ke database berikutnya.
// • Fallback ke local JSON storage jika tidak ada DATABASE_URL sama sekali.
//
// API publik modul ini SENGAJA tidak berubah agar seluruh kode lama
// (server.js, backupRouter.js, storageOptimizer.js, userDb.js, dll) tetap jalan.

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { fileURLToPath } from 'url';
import {
 withDb,
 initPool,
 hasAnyDatabase,
 getPoolStatus,
 rotateToNextSlot,
 checkQuotaAndRotate,
 startQuotaMonitor,
 stopQuotaMonitor,
 getMigrationHistory,
 getActiveSlotIndex,
 measureUsage,
 formatBytes,
 registerFallbackDataProvider,
 writeCrossSlotBackup,
 readCrossSlotBackup,
 CROSS_BACKUP_KEY
} from './dbPool.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
// Di Vercel disk proyek read-only; hanya /tmp yang bisa ditulis (sementara).
const localDataDir = process.env.VERCEL
  ? path.join(os.tmpdir(), 'bapperida-data')
  : path.join(__dirname, '..', 'data');
const localDbFile = path.join(localDataDir, 'local_db.json');
const backupDir = path.join(localDataDir, 'backups');

// Re-export agar router/script lain cukup import dari './lib/db.js'
export {
 initPool,
 getPoolStatus,
 rotateToNextSlot,
 checkQuotaAndRotate,
 startQuotaMonitor,
 stopQuotaMonitor,
 getMigrationHistory,
 measureUsage,
 formatBytes
};

function hasDatabaseUrl() {
 return hasAnyDatabase();
}

// ─── Local File Store (fallback tanpa DATABASE_URL) ─────────────────────────
function ensureDirectories() {
 if (!fs.existsSync(localDataDir)) fs.mkdirSync(localDataDir, { recursive: true });
 if (!fs.existsSync(backupDir)) fs.mkdirSync(backupDir, { recursive: true });
}

function readLocalStore() {
 try {
 ensureDirectories();
 if (!fs.existsSync(localDbFile)) {
 fs.writeFileSync(localDbFile, JSON.stringify({}, null, 2), 'utf-8');
 return {};
 }
 const content = fs.readFileSync(localDbFile, 'utf-8');
 return JSON.parse(content || '{}');
 } catch (err) {
 console.error('[DB Local] Gagal membaca local_db.json:', err.message);
 return {};
 }
}

let lastSnapshotTime = 0;
function createAutoSnapshot(data) {
 try {
 const now = Date.now();
 if (now - lastSnapshotTime < 5 * 60 * 1000) return;
 lastSnapshotTime = now;

 ensureDirectories();
 const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
 fs.writeFileSync(path.join(backupDir, `snapshot_${timestamp}.json`), JSON.stringify(data), 'utf-8');

 const files = fs.readdirSync(backupDir).filter(f => f.startsWith('snapshot_') && f.endsWith('.json'));
 if (files.length > 5) {
 files.sort().slice(0, files.length - 5).forEach(oldFile => {
 try { fs.unlinkSync(path.join(backupDir, oldFile)); } catch {}
 });
 }
 } catch (err) {
 console.warn('[DB Local] Warning snapshot backup:', err.message);
 }
}

function writeLocalStore(data) {
 try {
 ensureDirectories();
 const tempFile = path.join(localDataDir, `local_db_${Date.now()}.tmp`);
 fs.writeFileSync(tempFile, JSON.stringify(data), 'utf-8');
 fs.renameSync(tempFile, localDbFile);
 createAutoSnapshot(data);
 } catch (err) {
 console.error('[DB Local] Gagal menulis ke local_db.json:', err.message);
 }
}

// ─── Cadangan Aplikasi (dipakai saat Neon kena limit/kuota) ─────────────────
// Setiap kali data berhasil dibaca/ditulis dari Neon, salinannya disimpan ke
// disk lokal. Jika suatu saat kuota Neon habis lagi (semua slot database
// tidak bisa dibaca), aplikasi akan menyajikan salinan cadangan ini alih-alih
// melempar error teknis mentah ke pengguna — data yang ditampilkan adalah
// data terakhir yang berhasil "diunduh"/disinkronkan dari database, bukan
// data kosong. Mode ini hanya berlaku untuk pembacaan (read-only); penulisan
// baru tetap gagal secara eksplisit selama Neon belum bisa diakses, supaya
// tidak ada perubahan yang seolah-olah tersimpan padahal tidak.
const cloudBackupFile = path.join(backupDir, 'cloud_last_backup.json');
let cloudCache = null; // { savedAt, data: { [key]: any } }
let lastCloudCacheFlush = 0;
let servingFallbackData = false;

function loadCloudCache() {
 if (cloudCache) return cloudCache;
 try {
 ensureDirectories();
 if (fs.existsSync(cloudBackupFile)) {
 const parsed = JSON.parse(fs.readFileSync(cloudBackupFile, 'utf-8'));
 cloudCache = { savedAt: parsed.savedAt || null, data: parsed.data || {} };
 } else {
 cloudCache = { savedAt: null, data: {} };
 }
 } catch (err) {
 console.warn('[DB Cadangan] Gagal membaca cadangan lokal:', err.message);
 cloudCache = { savedAt: null, data: {} };
 }
 return cloudCache;
}

function persistCloudCache(force = false) {
 const now = Date.now();
 if (!force && now - lastCloudCacheFlush < 60 * 1000) return; // throttle 1 menit
 try {
 ensureDirectories();
 const cache = loadCloudCache();
 cache.savedAt = new Date().toISOString();
 const tempFile = path.join(localDataDir, `cloud_backup_${now}.tmp`);
 fs.writeFileSync(tempFile, JSON.stringify(cache), 'utf-8');
 fs.renameSync(tempFile, cloudBackupFile);
 lastCloudCacheFlush = now;

 // Disk lokal Render bersifat ephemeral (hilang tiap redeploy). Supaya
 // cadangan ini TETAP ADA lintas redeploy, replikasikan juga (best-effort,
 // tidak diblokir/di-await) ke beberapa slot Neon lain.
 if (hasAnyDatabase()) {
 writeCrossSlotBackup(cache.data, { excludeIndex: getActiveSlotIndex() }).catch(() => {});
 }
 } catch (err) {
 console.warn('[DB Cadangan] Gagal menyimpan cadangan lokal:', err.message);
 }
}

/** Ingat satu key (dipanggil setelah getStore/setStore berhasil ke Neon). */
function rememberKey(key, data) {
 if (key === '__db_pool_state' || key === '__db_migration_history' || key === CROSS_BACKUP_KEY) return;
 const cache = loadCloudCache();
 cache.data[key] = data;
 persistCloudCache(false);
}

/** Ingat seluruh data sekaligus (dipanggil setelah getAllStoreData berhasil). */
function rememberFull(fullData) {
 const cache = loadCloudCache();
 cache.data = { ...cache.data, ...fullData };
 persistCloudCache(true);
}

/** Info ringkas cadangan aplikasi — dipakai endpoint status/health. */
export function getFallbackBackupInfo() {
 const cache = loadCloudCache();
 const keys = Object.keys(cache.data || {});
 return { available: keys.length > 0, savedAt: cache.savedAt, keyCount: keys.length };
}

/** True jika respons TERAKHIR sedang disajikan dari cadangan lokal (bukan Neon langsung). */
export function isServingFallbackData() {
 return servingFallbackData;
}

// Daftarkan cadangan ini ke lib/dbPool.js supaya proses failover/migrasi
// internalnya juga bisa memakai snapshot ini sebagai jaring pengaman terakhir
// (mis. baru pertama kali kena kuota, belum pernah ada cadangan migrasi).
registerFallbackDataProvider(() => loadCloudCache().data);

// ─── API publik ─────────────────────────────────────────────────────────────

/**
 * Cek koneksi database aktif — dipakai endpoint /api/health.
 * Jika database aktif kena limit, withDb() otomatis melakukan failover
 * sehingga health check tetap hijau setelah pindah.
 */
export async function checkDbConnection() {
 if (!hasDatabaseUrl()) return 'local_file_store';
 return withDb(async (sql) => {
 const rows = await sql`SELECT NOW() AS now`;
 return rows[0]?.now;
 });
}

/**
 * Ambil satu "dokumen" JSON berdasarkan key. Return null jika belum ada.
 *
 * Jika Neon tidak dapat diakses (mis. kuota/limit habis) DAN failover
 * otomatis (lib/dbPool.js) juga gagal, fungsi ini TIDAK langsung melempar
 * error teknis mentah — melainkan mencoba menyajikan cadangan lokal (data
 * terakhir yang berhasil disinkronkan). Aplikasi jadi tetap bisa
 * menampilkan data (mode baca-saja) alih-alih error/kosong.
 */
export async function getStore(key) {
 if (!hasDatabaseUrl()) {
 const localDb = readLocalStore();
 return localDb[key] ?? null;
 }
 try {
 const result = await withDb(async (sql) => {
 await sql`
 CREATE TABLE IF NOT EXISTS app_store (
 key TEXT PRIMARY KEY,
 data JSONB NOT NULL,
 updated_at TIMESTAMPTZ DEFAULT NOW()
 )
 `;
 const rows = await sql`SELECT data FROM app_store WHERE key = ${key}`;
 return rows[0]?.data ?? null;
 });
 servingFallbackData = false;
 if (result !== null && result !== undefined) rememberKey(key, result);
 return result;
 } catch (err) {
 console.error(`[DB] Gagal membaca "${key}" dari database:`, err.message);
 const cache = loadCloudCache();
 if (cache.data && Object.prototype.hasOwnProperty.call(cache.data, key)) {
 servingFallbackData = true;
 console.warn(`[DB] Kuota/limit database terdampak — menyajikan cadangan lokal untuk "${key}" (disimpan ${cache.savedAt}).`);
 return cache.data[key];
 }
 // Cadangan lokal (disk Render) mungkin baru saja ter-reset oleh redeploy —
 // coba cadangan yang direplikasi ke slot Neon lain sebagai upaya terakhir.
 try {
 const cross = await readCrossSlotBackup();
 if (cross && cross.data && Object.prototype.hasOwnProperty.call(cross.data, key)) {
 servingFallbackData = true;
 console.warn(`[DB] Menyajikan cadangan lintas-slot untuk "${key}" (slot #${cross.foundAtSlot}, disimpan ${cross.savedAt}).`);
 rememberKey(key, cross.data[key]);
 return cross.data[key];
 }
 } catch {}
 throw new Error(
 `Database sedang tidak dapat diakses (kemungkinan kuota terlampaui) dan belum ada cadangan lokal untuk "${key}". Silakan coba lagi beberapa saat lagi.`
 );
 }
}

/**
 * Simpan (insert atau update) satu "dokumen" JSON berdasarkan key.
 *
 * Penulisan TIDAK memiliki mode fallback: jika Neon tidak dapat diakses,
 * fungsi ini tetap melempar error (dengan pesan yang lebih ramah) supaya
 * pengguna tahu perubahannya belum tersimpan, alih-alih diam-diam dianggap
 * berhasil padahal sebenarnya hilang.
 */
/**
 * Probe ringan: hanya membaca penanda waktu perubahan (updated_at) sebuah key,
 * tanpa menarik seluruh isi JSONB. Dipakai poller realtime & optimizer untuk
 * memutuskan apakah pembacaan penuh memang diperlukan.
 * Return string penanda, atau null bila tidak diketahui (mis. file lokal / error)
 * — pemanggil harus menganggap null sebagai "mungkin berubah".
 */
export async function getStoreUpdatedAt(key) {
  if (!hasDatabaseUrl()) return null;
  try {
    return await withDb(async (sql) => {
      const rows = await sql`SELECT updated_at::text AS v FROM app_store WHERE key = ${key}`;
      return rows[0]?.v ?? null;
    });
  } catch {
    return null;
  }
}

export async function setStore(key, data) {
 if (!hasDatabaseUrl()) {
 const localDb = readLocalStore();
 localDb[key] = data;
 writeLocalStore(localDb);
 return;
 }
 const json = JSON.stringify(data);
 try {
 await withDb(async (sql) => {
 await sql`
 CREATE TABLE IF NOT EXISTS app_store (
 key TEXT PRIMARY KEY,
 data JSONB NOT NULL,
 updated_at TIMESTAMPTZ DEFAULT NOW()
 )
 `;
 await sql`
 INSERT INTO app_store (key, data, updated_at)
 VALUES (${key}, ${json}, NOW())
 ON CONFLICT (key) DO UPDATE
 SET data = ${json}, updated_at = NOW()
 `;
 });
 servingFallbackData = false;
 rememberKey(key, data);
 } catch (err) {
 console.error(`[DB] Gagal menyimpan "${key}" ke database:`, err.message);
 throw new Error(
 `Gagal menyimpan data ke database (kemungkinan kuota terlampaui). Perubahan BELUM tersimpan — coba lagi beberapa saat lagi. Detail teknis: ${err.message}`
 );
 }
}

/**
 * Ambil seluruh data database untuk Full Backup.
 * Sama seperti getStore(): jika Neon & failover otomatis sama-sama gagal,
 * kembalikan cadangan lokal terakhir (jika ada) daripada melempar error.
 */
export async function getAllStoreData() {
 if (!hasDatabaseUrl()) return readLocalStore();
 try {
 const full = await withDb(async (sql) => {
 const rows = await sql`SELECT key, data FROM app_store`;
 const result = {};
 for (const row of rows) {
 // Key internal pool tidak ikut diekspor ke file backup pengguna
 if (row.key === '__db_pool_state' || row.key === '__db_migration_history' || row.key === CROSS_BACKUP_KEY) continue;
 result[row.key] = row.data;
 }
 return result;
 });
 servingFallbackData = false;
 rememberFull(full);
 return full;
 } catch (err) {
 console.error('[DB] Gagal mengambil seluruh data database:', err.message);
 const cache = loadCloudCache();
 if (cache.data && Object.keys(cache.data).length > 0) {
 servingFallbackData = true;
 console.warn(`[DB] Kuota/limit database terdampak — menyajikan seluruh cadangan lokal (disimpan ${cache.savedAt}).`);
 return { ...cache.data };
 }
 try {
 const cross = await readCrossSlotBackup();
 if (cross && cross.data && Object.keys(cross.data).length > 0) {
 servingFallbackData = true;
 console.warn(`[DB] Menyajikan seluruh cadangan lintas-slot (slot #${cross.foundAtSlot}, disimpan ${cross.savedAt}).`);
 rememberFull(cross.data);
 return { ...cross.data };
 }
 } catch {}
 throw new Error(
 'Database tidak dapat diakses (kemungkinan kuota terlampaui) dan belum ada cadangan lokal yang tersimpan.'
 );
 }
}

/** Restore seluruh data database dari Backup JSON. */
export async function restoreAllStoreData(fullData) {
 if (!fullData || typeof fullData !== 'object') {
 throw new Error('Data backup tidak valid.');
 }
 if (!hasDatabaseUrl()) {
 writeLocalStore(fullData);
 return true;
 }
 return withDb(async (sql) => {
 for (const [key, data] of Object.entries(fullData)) {
 if (key === '__db_pool_state' || key === CROSS_BACKUP_KEY) continue; // jangan timpa state pool / cadangan lintas-slot
 const json = JSON.stringify(data);
 await sql`
 INSERT INTO app_store (key, data, updated_at)
 VALUES (${key}, ${json}, NOW())
 ON CONFLICT (key) DO UPDATE
 SET data = ${json}, updated_at = NOW()
 `;
 }
 return true;
 });
}

/** Daftar file snapshot lokal (mencakup snapshot rutin & cadangan migrasi). */
export function getBackupSnapshotsList() {
 try {
 ensureDirectories();
 const migrationDir = path.join(localDataDir, 'migrations');
 const entries = [];

 const collect = (dir, kind) => {
 if (!fs.existsSync(dir)) return;
 for (const filename of fs.readdirSync(dir).filter(f => f.endsWith('.json'))) {
 const stat = fs.statSync(path.join(dir, filename));
 entries.push({ filename, kind, size: stat.size, createdAt: stat.birthtime || stat.mtime });
 }
 };

 collect(backupDir, 'snapshot');
 collect(migrationDir, 'migration');

 return entries.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
 } catch {
 return [];
 }
}

/** Ringkasan storage database aktif — dipakai endpoint /api/v1/backup/stats. */
export async function getActiveStorageInfo() {
 if (!hasDatabaseUrl()) {
 return { type: 'Local Persistent Storage', slot: null, usedLabel: null, usagePercent: null };
 }
 try {
 await initPool();
 const idx = getActiveSlotIndex();
 const usage = await measureUsage(idx);
 return {
 type: 'Neon PostgreSQL (Multi-DB Failover)',
 slot: idx + 1,
 usedBytes: usage.bytes,
 usedLabel: formatBytes(usage.bytes),
 quotaLabel: formatBytes(usage.quotaBytes),
 usagePercent: Number((usage.ratio * 100).toFixed(1)),
 fallback: getFallbackBackupInfo(),
 servingFallback: isServingFallbackData()
 };
 } catch (err) {
 return {
 type: 'Neon PostgreSQL (Multi-DB Failover)',
 error: err.message,
 fallback: getFallbackBackupInfo(),
 servingFallback: isServingFallbackData()
 };
 }
}
