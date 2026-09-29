// routes/backupRouter.js
// Router Backup & Restore Bertingkat (Admin, Moderator, User)

import express from 'express';
import { requireAuth, requireRole } from '../auth/authMiddleware.js';
import { getAllStoreData, restoreAllStoreData, getBackupSnapshotsList, getStore, setStore, getActiveStorageInfo } from '../lib/db.js';
import { logActivity } from '../utils/activityLogger.js';
import { realtimeHub } from '../utils/realtimeHub.js';

// Urutkan dokumen RKA: unggahan terbaru (tanggal & jam) di paling awal.
// Dipakai setelah restore/gabung backup supaya urutan tersimpan konsisten.
function rkiTime(r) {
 const raw = r?.tanggalUpload || r?.createdAt || r?.uploadedAt || r?.timestamp;
 const t = raw ? Date.parse(raw) : NaN;
 return Number.isNaN(t) ? 0 : t;
}
function sortRkisNewestFirst(list) {
 return Array.isArray(list) ? [...list].sort((a, b) => rkiTime(b) - rkiTime(a)) : list;
}

const router = express.Router();

/**
 * 1. Full Database Export (Admin & Moderator)
 * Mengunduh seluruh data (RKA, SSH, User, Konfigurasi API, dan Log) dalam 1 berkas JSON
 */
router.get('/export-full', requireAuth, requireRole('admin', 'moderator'), async (req, res) => {
 try {
 const rawData = await getAllStoreData();

 // Jangan sertakan hash password user di export jika role adalah moderator
 const safeData = JSON.parse(JSON.stringify(rawData));
 if (req.user.role === 'moderator' && safeData.users_db?.users) {
 safeData.users_db.users = safeData.users_db.users.map(u => {
 const { password, ...rest } = u;
 return rest;
 });
 }

 const payload = {
 app: 'Sintra / Bapperida RKA AI',
 version: '2.0.0',
 exportedAt: new Date().toISOString(),
 exportedBy: {
 id: req.user.id,
 username: req.user.username,
 name: req.user.name,
 role: req.user.role
 },
 data: safeData
 };

 await logActivity({
 req,
 action: 'EXPORT_BACKUP',
 target: 'Full Database Backup',
 details: `Ekspor database penuh oleh ${req.user.role} (${req.user.username})`
 });

 res.setHeader('Content-Type', 'application/json');
 res.setHeader('Content-Disposition', `attachment; filename="sintra_backup_full_${new Date().toISOString().slice(0, 10)}.json"`);
 res.json(payload);
 } catch (err) {
 console.error('Export full error:', err);
 res.status(500).json({ error: 'Gagal membuat file backup: ' + err.message });
 }
});

/**
 * 2. User Data Export (Semua User & Moderator)
 * Mengunduh berkas RKA milik pengguna (atau seluruh RKA jika moderator/admin)
 * Payload memiliki wrapper `data` agar kompatibel dengan endpoint /restore
 * (sebelumnya wrapper ini tidak ada sehingga tombol "Upload Backup .html"
 * selalu gagal dengan error "Harus mengandung field data").
 */
router.get('/export-user', requireAuth, async (req, res) => {
 try {
 const mainDb = await getStore('main_db') || { rkis: [] };
 let rkis = mainDb.rkis || [];

 // Jika user biasa, ambil hanya dokumen miliknya
 const isUserRole = req.user.role === 'user';
 if (isUserRole) {
 rkis = rkis.filter(r => !r.userId || r.userId === req.user.id);
 }

 const payload = {
 app: 'Sintra / Bapperida RKA AI',
 type: 'user_rka_backup',
 exportedAt: new Date().toISOString(),
 exportedBy: {
 id: req.user.id,
 username: req.user.username,
 name: req.user.name,
 role: req.user.role
 },
 user: {
 id: req.user.id,
 username: req.user.username,
 name: req.user.name
 },
 totalDocuments: rkis.length,
 // Field langsung (dipakai oleh htmlBackupGenerator & kompatibilitas lama)
 rkis,
 // Wrapper `data` (dibutuhkan oleh endpoint /restore)
 data: {
 main_db: {
 rkis,
 ssh_databases: isUserRole ? [] : (mainDb.ssh_databases || [])
 }
 }
 };

 await logActivity({
 req,
 action: 'EXPORT_BACKUP',
 target: `User RKA (${rkis.length} Dokumen)`,
 details: `Ekspor data RKA oleh ${req.user.username}`
 });

 res.setHeader('Content-Type', 'application/json');
 res.setHeader('Content-Disposition', `attachment; filename="rka_user_${req.user.username}_${new Date().toISOString().slice(0, 10)}.json"`);
 res.json(payload);
 } catch (err) {
 console.error('Export user data error:', err);
 res.status(500).json({ error: 'Gagal mengekspor data RKA pengguna: ' + err.message });
 }
});

/**
 * 3. Restore Database
 * - Admin & Moderator: restore penuh (seluruh main_db), butuh field `data`.
 * - User biasa: merge hanya dokumen RKA miliknya sendiri ke dalam DB yang ada.
 * (Sebelumnya endpoint ini dikunci requireRole('admin','moderator') sehingga
 * tombol "Upload Backup .html" milik user biasa di halaman Arsip Dokumen
 * selalu gagal dengan 403 sebelum sempat memeriksa isi berkas.)
 */
router.post('/restore', requireAuth, async (req, res) => {
 try {
 const backupPayload = req.body;
 const isAdminOrMod = req.user.role === 'admin' || req.user.role === 'moderator';

 if (isAdminOrMod) {
 if (!backupPayload || !backupPayload.data) {
 return res.status(400).json({ error: 'Format berkas backup tidak valid. Harus mengandung field "data".' });
 }
 if (Array.isArray(backupPayload.data?.main_db?.rkis)) {
 backupPayload.data.main_db.rkis = sortRkisNewestFirst(backupPayload.data.main_db.rkis);
 }
 await restoreAllStoreData(backupPayload.data);
 } else {
 // User biasa: hanya boleh merge RKA miliknya sendiri
 const incomingRkis = backupPayload?.rkis
 || backupPayload?.data?.main_db?.rkis
 || [];

 if (!Array.isArray(incomingRkis) || incomingRkis.length === 0) {
 return res.status(400).json({ error: 'Tidak ada dokumen RKA ditemukan dalam berkas backup.' });
 }

 const userRkis = incomingRkis.filter(r => !r.userId || r.userId === req.user.id);
 if (userRkis.length === 0) {
 return res.status(403).json({ error: 'Berkas backup tidak mengandung dokumen RKA milik akun Anda.' });
 }

 const mainDb = await getStore('main_db') || { rkis: [], ssh_databases: [] };
 const existingRkis = mainDb.rkis || [];
 const idMap = new Map(existingRkis.map(r => [r.id, r]));
 for (const rki of userRkis) {
 idMap.set(rki.id, { ...rki, userId: req.user.id }); // pastikan kepemilikan tetap benar
 }
 mainDb.rkis = sortRkisNewestFirst(Array.from(idMap.values()));
 await restoreAllStoreData({ main_db: mainDb });
 }

 // Beri tahu semua browser yang sedang terbuka agar memuat ulang arsip otomatis (tanpa refresh halaman)
 realtimeHub.broadcast('ARSIP_SYNC', { reason: 'restore', at: new Date().toISOString() });

 await logActivity({
 req,
 action: 'RESTORE_BACKUP',
 target: isAdminOrMod ? 'Database Restored (Full)' : 'RKA Restored (User Merge)',
 details: `Pemulihan dari berkas backup tanggal ${backupPayload?.exportedAt || 'tidak diketahui'} oleh ${req.user.username}`
 });

 res.json({
 success: true,
 message: isAdminOrMod
 ? 'Database berhasil dipulihkan dari file backup.'
 : 'Dokumen RKA Anda berhasil dipulihkan dan digabungkan ke dalam arsip.',
 restoredAt: new Date().toISOString()
 });
 } catch (err) {
 console.error('Restore error:', err);
 res.status(500).json({ error: 'Gagal memulihkan database: ' + err.message });
 }
});

/**
 * 3b. Tambah ke Arsip (Merge) — TIDAK menghapus dokumen yang sudah ada.
 * Dipakai untuk mengunggah berkas backup satu per satu (hasil ekstrak .html
 * maupun .json). Hanya dokumen RKA (main_db.rkis) yang disentuh; dokumen lain
 * di arsip, ssh_databases, akun pengguna, konfigurasi, dan log tetap utuh.
 *
 * Body: { rkis: [...], overwrite?: boolean, sourceName?: string }
 * - Dokumen dengan ID baru  → ditambahkan.
 * - Dokumen dengan ID sudah ada → dilewati, kecuali overwrite=true (hanya
 *   dokumen itu yang diperbarui, sisanya tidak disentuh).
 */
router.post('/import-merge', requireAuth, async (req, res) => {
 try {
 const { rkis: incoming, overwrite = false, sourceName } = req.body || {};
 if (!Array.isArray(incoming) || incoming.length === 0) {
 return res.status(400).json({ error: 'Tidak ada dokumen RKA yang ditemukan dalam berkas.' });
 }

 const isAdminOrMod = req.user.role === 'admin' || req.user.role === 'moderator';
 const mainDb = (await getStore('main_db')) || { rkis: [], ssh_databases: [] };
 const existing = Array.isArray(mainDb.rkis) ? mainDb.rkis : [];
 const byId = new Map(existing.map(r => [r.id, r]));

 let added = 0, updated = 0, skipped = 0, forbidden = 0, invalid = 0;
 const addedNames = [];

 for (const raw of incoming) {
 if (!raw || typeof raw !== 'object' || !raw.id) { invalid++; continue; }

 // User biasa hanya boleh memasukkan dokumen tanpa pemilik / miliknya sendiri
 if (!isAdminOrMod && raw.userId && raw.userId !== req.user.id) { forbidden++; continue; }

 // Dokumen tanpa pemilik dianggap milik pengunggah agar tidak terlihat oleh semua user
 const doc = { ...raw, userId: raw.userId || req.user.id };
 const prev = byId.get(doc.id);

 if (!prev) {
 byId.set(doc.id, doc);
 existing.push(doc);
 added++;
 addedNames.push(doc.namaDokumen || doc.id);
 } else if (overwrite) {
 // Jangan biarkan user biasa menimpa dokumen milik orang lain
 if (!isAdminOrMod && prev.userId && prev.userId !== req.user.id) { forbidden++; continue; }
 const idx = existing.findIndex(r => r.id === doc.id);
 existing[idx] = { ...doc, userId: prev.userId || doc.userId };
 byId.set(doc.id, existing[idx]);
 updated++;
 } else {
 skipped++;
 }
 }

 if (added > 0 || updated > 0) {
 mainDb.rkis = sortRkisNewestFirst(existing);
 await setStore('main_db', mainDb); // hanya key main_db; store lain tidak disentuh
 realtimeHub.broadcast('ARSIP_SYNC', { reason: 'import-merge', added, updated, at: new Date().toISOString() });
 }

 await logActivity({
 req,
 action: 'RESTORE_BACKUP',
 target: `Tambah Arsip dari Backup (${sourceName || 'berkas'})`,
 details: `Gabung backup: ${added} ditambahkan, ${updated} diperbarui, ${skipped} dilewati (sudah ada) oleh ${req.user.username}`
 });

 res.json({
 success: true,
 added, updated, skipped, forbidden, invalid,
 total: existing.length,
 addedNames: addedNames.slice(0, 20)
 });
 } catch (err) {
 console.error('Import-merge error:', err);
 res.status(500).json({ error: 'Gagal menambahkan dokumen ke arsip: ' + err.message });
 }
});

/**
 * 4. Backup Stats & Snapshots List
 */
router.get('/stats', requireAuth, requireRole('admin', 'moderator'), async (req, res) => {
 try {
 const mainDb = await getStore('main_db') || { rkis: [], ssh_databases: [] };
 const usersDb = await getStore('users_db') || { users: [] };
 const logsDb = await getStore('activity_logs') || { logs: [] };
 const snapshots = getBackupSnapshotsList();
 const storage = await getActiveStorageInfo();

 res.json({
 totalRki: mainDb.rkis?.length || 0,
 totalUsers: usersDb.users?.length || 0,
 totalLogs: logsDb.logs?.length || 0,
 totalSnapshots: snapshots.length,
 snapshots: snapshots.slice(0, 10),
 storageType: storage.type,
 storage // { slot, usedLabel, quotaLabel, usagePercent }
 });
 } catch (err) {
 res.status(500).json({ error: 'Gagal memuat status backup: ' + err.message });
 }
});

export default router;
