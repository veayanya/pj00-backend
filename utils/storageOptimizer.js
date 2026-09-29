// utils/storageOptimizer.js
// Modul optimasi storage otomatis — berjalan berkala (default 10 menit) dan hanya bila data berubah.
// Mengkompresi & memangkas data agar hemat storage dan energi.

import { getStore, setStore, getStoreUpdatedAt } from '../lib/db.js';

// ─── Konfigurasi ──────────────────────────────────────────────────────────────
// Default 10 menit (sebelumnya 60 detik). Tiap siklus dilewati bila data tidak berubah
// sejak siklus terakhir, jadi database tidak dibaca sia-sia saat aplikasi sepi.
const OPTIMIZER_INTERVAL_MS = Math.max(60_000, Number(process.env.STORAGE_OPTIMIZER_INTERVAL_MS) || 10 * 60 * 1000);
const WATCHED_KEYS = ['main_db', 'activity_logs'];
let lastSeenVersions = null;

async function readVersions() {
  const v = await Promise.all(WATCHED_KEYS.map((k) => getStoreUpdatedAt(k)));
  return v.some((x) => x === null) ? null : v.join('|'); // null = tidak diketahui → jalankan
}
const LOG_MAX_AGE_FULL_MS = 7 * 24 * 3600e3; // 7 hari → log dipangkas (hapus details/ip)
const LOG_MAX_AGE_DELETE_MS = 30 * 24 * 3600e3; // 30 hari → log dihapus total
const LOG_HARD_LIMIT = 500; // batas keras jumlah log
const RKA_RAW_TEXT_AGE_MS = 24 * 3600e3; // 24 jam → rawText dihapus

// ─── Statistik ────────────────────────────────────────────────────────────────
const stats = {
 lastRun: null,
 totalRuns: 0,
 logsCompacted: 0,
 logsDeleted: 0,
 rkaTextsTrimmed: 0,
 sshDuplicatesRemoved: 0,
 totalBytesSaved: 0
};

let optimizerTimer = null;

/**
 * 1. Kompaksi Activity Logs
 * - Log > 30 hari: hapus total
 * - Log > 7 hari: pangkas field verbose (details, ip) → hemat ~60% per entry
 * - Hard limit: 500 log terakhir
 */
async function compactActivityLogs() {
 let compacted = 0;
 let deleted = 0;

 try {
 const logDb = await getStore('activity_logs');
 if (!logDb || !Array.isArray(logDb.logs) || logDb.logs.length === 0) return { compacted, deleted };

 const now = Date.now();
 const originalSize = JSON.stringify(logDb.logs).length;

 // Fase 1: Hapus log > 30 hari
 logDb.logs = logDb.logs.filter(log => {
 const age = now - new Date(log.timestamp).getTime();
 if (age > LOG_MAX_AGE_DELETE_MS) {
 deleted++;
 return false;
 }
 return true;
 });

 // Fase 2: Pangkas field verbose pada log > 7 hari
 for (const log of logDb.logs) {
 const age = now - new Date(log.timestamp).getTime();
 if (age > LOG_MAX_AGE_FULL_MS) {
 if (log.details && log.details.length > 0) {
 // Simpan hanya 50 karakter pertama sebagai ringkasan
 log.details = log.details.substring(0, 50) + (log.details.length > 50 ? '...' : '');
 compacted++;
 }
 // Hapus IP dari log tua
 if (log.ip) delete log.ip;
 }
 }

 // Fase 3: Terapkan hard limit
 if (logDb.logs.length > LOG_HARD_LIMIT) {
 deleted += logDb.logs.length - LOG_HARD_LIMIT;
 logDb.logs = logDb.logs.slice(0, LOG_HARD_LIMIT);
 }

 // Simpan hanya jika ada perubahan
 if (compacted > 0 || deleted > 0) {
 await setStore('activity_logs', logDb);
 const newSize = JSON.stringify(logDb.logs).length;
 stats.totalBytesSaved += Math.max(0, originalSize - newSize);
 }

 stats.logsCompacted += compacted;
 stats.logsDeleted += deleted;
 } catch (err) {
 console.warn('[StorageOptimizer] compactActivityLogs error:', err.message);
 }

 return { compacted, deleted };
}

/**
 * 2. Trim RKA Raw Text
 * - Hapus field teks mentah PDF yang tidak diperlukan lagi setelah analisis AI
 * - Hanya untuk dokumen RKA yang sudah > 24 jam sejak dibuat
 */
async function trimRkaRawText() {
 let trimmed = 0;

 try {
 const db = await getStore('main_db');
 if (!db || !Array.isArray(db.rkis) || db.rkis.length === 0) return { trimmed };

 const now = Date.now();
 let changed = false;

 // Field yang mengandung teks mentah besar dan tidak diperlukan setelah analisis
 const rawTextFields = ['_rawText', 'rawPdfText', 'rawText', 'originalText', 'extractedText'];

 for (const rka of db.rkis) {
 // Cek usia dokumen dari timestamp pembuatan
 const createdAt = rka.createdAt || rka.uploadedAt || rka.tanggalUpload;
 if (!createdAt) continue;

 const age = now - new Date(createdAt).getTime();
 if (age < RKA_RAW_TEXT_AGE_MS) continue; // Masih segar, skip

 for (const field of rawTextFields) {
 if (rka[field] && typeof rka[field] === 'string' && rka[field].length > 100) {
 delete rka[field];
 trimmed++;
 changed = true;
 }
 }

 // Kompres field 'outcome_description' yang panjang > 500 karakter
 if (rka.outcome_description && rka.outcome_description.length > 500) {
 rka.outcome_description = rka.outcome_description.substring(0, 500) + '...';
 trimmed++;
 changed = true;
 }

 // Kompres field 'alasan' evaluasi yang terlalu panjang di sub-objek evaluasi_rka
 if (rka.evaluasi_rka && typeof rka.evaluasi_rka === 'object') {
 for (const [, aspek] of Object.entries(rka.evaluasi_rka)) {
 if (aspek && typeof aspek === 'object') {
 for (const key of ['alasan', 'temuan', 'risiko', 'rekomendasi']) {
 if (aspek[key] && typeof aspek[key] === 'string' && aspek[key].length > 300) {
 aspek[key] = aspek[key].substring(0, 300) + '...';
 changed = true;
 }
 }
 }
 }
 }
 }

 if (changed) {
 await setStore('main_db', db);
 }

 stats.rkaTextsTrimmed += trimmed;
 } catch (err) {
 console.warn('[StorageOptimizer] trimRkaRawText error:', err.message);
 }

 return { trimmed };
}

/**
 * 3. Deduplikasi SSH Items pada Arsip
 * - Hanya proses SSH databases berstatus 'archived'
 * - Hapus item duplikat (nama + harga_satuan identik)
 */
async function deduplicateSshItems() {
 let removed = 0;

 try {
 const db = await getStore('main_db');
 if (!db || !Array.isArray(db.ssh_databases) || db.ssh_databases.length === 0) return { removed };

 let changed = false;

 for (const ssh of db.ssh_databases) {
 if (ssh.status !== 'archived') continue;
 if (!Array.isArray(ssh.items) || ssh.items.length === 0) continue;

 const originalCount = ssh.items.length;
 const seen = new Set();
 const deduped = [];

 for (const item of ssh.items) {
 // Buat fingerprint dari nama + harga untuk deteksi duplikat
 const key = `${(item.nama || item.uraian || '').toLowerCase().trim()}|${item.harga_satuan || item.harga || 0}`;
 if (!seen.has(key)) {
 seen.add(key);
 deduped.push(item);
 }
 }

 if (deduped.length < originalCount) {
 const diff = originalCount - deduped.length;
 removed += diff;
 ssh.items = deduped;
 ssh.total_item = deduped.length;
 changed = true;
 }
 }

 if (changed) {
 await setStore('main_db', db);
 }

 stats.sshDuplicatesRemoved += removed;
 } catch (err) {
 console.warn('[StorageOptimizer] deduplicateSshItems error:', err.message);
 }

 return { removed };
}

/**
 * Orchestrator — dipanggil tiap 60 detik
 */
export async function runStorageOptimizer() {
const startTime = Date.now();

try {
const before = await readVersions();
if (before !== null && before === lastSeenVersions) return; // tidak ada perubahan → lewati

 const logResult = await compactActivityLogs();
 const rkaResult = await trimRkaRawText();
 const sshResult = await deduplicateSshItems();

 stats.lastRun = new Date().toISOString();
stats.totalRuns++;
// Catat penanda SETELAH siklus (perubahan hasil optimasi sendiri tidak memicu siklus ulang).
lastSeenVersions = await readVersions();

 const hasChanges = logResult.compacted > 0 || logResult.deleted > 0 ||
 rkaResult.trimmed > 0 || sshResult.removed > 0;

 if (hasChanges) {
 const elapsed = Date.now() - startTime;
 console.log(
 `[StorageOptimizer] Siklus #${stats.totalRuns} selesai (${elapsed}ms) — ` +
 `Log: ${logResult.compacted} dipangkas, ${logResult.deleted} dihapus | ` +
 `RKA: ${rkaResult.trimmed} teks dipangkas | ` +
 `SSH: ${sshResult.removed} duplikat dihapus`
 );
 }
 } catch (err) {
 console.error('[StorageOptimizer] Error pada siklus optimasi:', err.message);
 }
}

/**
 * Mulai interval optimizer (dipanggil saat server boot)
 */
export function startStorageOptimizer() {
 if (optimizerTimer) return; // Sudah berjalan

 console.log(`[StorageOptimizer] Dimulai — interval setiap ${OPTIMIZER_INTERVAL_MS / 1000} detik`);

 // Jalankan pertama kali setelah 10 detik (beri waktu server init)
 setTimeout(() => {
 runStorageOptimizer();
 optimizerTimer = setInterval(runStorageOptimizer, OPTIMIZER_INTERVAL_MS);
 }, 10_000);
}

/**
 * Hentikan optimizer (untuk graceful shutdown)
 */
export function stopStorageOptimizer() {
 if (optimizerTimer) {
 clearInterval(optimizerTimer);
 optimizerTimer = null;
 console.log('[StorageOptimizer] Dihentikan.');
 }
}

/**
 * Ambil statistik optimizer untuk endpoint monitoring
 */
export function getOptimizerStats() {
 return {
 ...stats,
 isRunning: !!optimizerTimer,
 intervalSeconds: OPTIMIZER_INTERVAL_MS / 1000,
 config: {
 logMaxAgeFull: '7 hari',
 logMaxAgeDelete: '30 hari',
 logHardLimit: LOG_HARD_LIMIT,
 rkaRawTextAge: '24 jam'
 }
 };
}
