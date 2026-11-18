const fs = require('fs');
const os = require('os');
const path = require('path');
const zlib = require('zlib');

const targetVersion = '0.1.0';
const targetPublisher = 'rogeriocenteno';
const targetAuthor = {
  name: 'Rogério Centeno',
  url: 'https://github.com/rogeriocenteno'
};
const targetDescription = 'Extensão para monitoramento em tempo real do consumo de tokens e cotas de IA (5h/7d) no Antigravity IDE 2.6.0+.';
const targetRepoUrl = 'https://github.com/rogeriocenteno/MedidorTokens';

const extensionsRoot = path.join(os.homedir(), '.antigravity-ide', 'extensions');
const allEntries = fs.existsSync(extensionsRoot) ? fs.readdirSync(extensionsRoot) : [];
const ownDirs = allEntries.filter((name) => name.startsWith(`${targetPublisher}.altimeter-`)).sort();
const legacyDirs = allEntries.filter((name) => name.startsWith('pqub.altimeter-')).sort();
const altimeterDirs = ownDirs.length > 0 ? ownDirs : legacyDirs;

if (altimeterDirs.length === 0) {
  throw new Error('Pasta da extensão altimeter não encontrada em: ' + extensionsRoot);
}

const targetFolderName = `${targetPublisher}.altimeter-${targetVersion}`;
const sourceExtDir = path.join(
  extensionsRoot,
  ownDirs.includes(targetFolderName) ? targetFolderName : altimeterDirs[altimeterDirs.length - 1]
);
const extDir = path.join(extensionsRoot, targetFolderName);
const outDir = __dirname;
const distBackupDir = path.join(outDir, 'dist_altimeter_corrigido');

fs.mkdirSync(distBackupDir, { recursive: true });

function copyDirRecursive(src, dest) {
  fs.mkdirSync(dest, { recursive: true });
  for (const item of fs.readdirSync(src, { withFileTypes: true })) {
    const srcPath = path.join(src, item.name);
    const destPath = path.join(dest, item.name);
    if (item.isDirectory()) {
      copyDirRecursive(srcPath, destPath);
    } else {
      fs.copyFileSync(srcPath, destPath);
    }
  }
}

if (sourceExtDir !== extDir) {
  copyDirRecursive(sourceExtDir, extDir);
  console.log('Migrada pasta da extensão para:', extDir);
}

// 1. Sincroniza o extension.js mais recente entre a pasta do projeto e a extensao instalada
const sourceJsCandidate = path.join(sourceExtDir, 'dist', 'extension.js');
const extJsPath = path.join(extDir, 'dist', 'extension.js');
const backupExtJsPath = path.join(distBackupDir, 'extension.js');
fs.mkdirSync(path.dirname(extJsPath), { recursive: true });

if (fs.existsSync(sourceJsCandidate) && fs.existsSync(backupExtJsPath)) {
  const extStat = fs.statSync(sourceJsCandidate);
  const bkpStat = fs.statSync(backupExtJsPath);
  if (extStat.mtimeMs > bkpStat.mtimeMs) {
    fs.copyFileSync(sourceJsCandidate, backupExtJsPath);
    if (sourceJsCandidate !== extJsPath) {
      fs.copyFileSync(backupExtJsPath, extJsPath);
    }
    console.log('Copiado da IDE para o projeto:', backupExtJsPath);
  } else {
    try {
      fs.copyFileSync(backupExtJsPath, extJsPath);
      console.log('Copiado do projeto para a IDE:', extJsPath);
    } catch {
      console.log('Usando versao do projeto em:', backupExtJsPath);
    }
  }
} else if (fs.existsSync(sourceJsCandidate)) {
  fs.copyFileSync(sourceJsCandidate, backupExtJsPath);
  if (sourceJsCandidate !== extJsPath) {
    fs.copyFileSync(backupExtJsPath, extJsPath);
  }
} else if (fs.existsSync(backupExtJsPath)) {
  fs.copyFileSync(backupExtJsPath, extJsPath);
}
const sourceJsForVsix = fs.existsSync(backupExtJsPath) ? backupExtJsPath : extJsPath;

// Implementacao CRC32 para geracao de arquivo ZIP (.vsix) padrao
const crcTable = new Uint32Array(256);
for (let n = 0; n < 256; n++) {
  let c = n;
  for (let k = 0; k < 8; k++) {
    c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
  }
  crcTable[n] = c >>> 0;
}

function crc32(buf) {
  let c = 0xFFFFFFFF;
  for (let i = 0; i < buf.length; i++) {
    c = crcTable[(c ^ buf[i]) & 0xFF] ^ (c >>> 8);
  }
  return (c ^ 0xFFFFFFFF) >>> 0;
}

function createZip(entries) {
  const localHeaders = [];
  const centralHeaders = [];
  let offset = 0;

  for (const entry of entries) {
    const nameBuf = Buffer.from(entry.name.replace(/\\/g, '/'), 'utf8');
    const rawData = Buffer.isBuffer(entry.data) ? entry.data : Buffer.from(entry.data, 'utf8');
    const compressed = zlib.deflateRawSync(rawData, { level: 9 });
    const crc = crc32(rawData);

    const localHeader = Buffer.alloc(30 + nameBuf.length);
    localHeader.writeUInt32LE(0x04034b50, 0); // signature
    localHeader.writeUInt16LE(20, 4); // version needed
    localHeader.writeUInt16LE(0, 6); // flags
    localHeader.writeUInt16LE(8, 8); // compression method: deflate
    localHeader.writeUInt16LE(0, 10); // mod time
    localHeader.writeUInt16LE(0x5d5c, 12); // mod date (2026-10-28)
    localHeader.writeUInt32LE(crc, 14);
    localHeader.writeUInt32LE(compressed.length, 18);
    localHeader.writeUInt32LE(rawData.length, 22);
    localHeader.writeUInt16LE(nameBuf.length, 26);
    localHeader.writeUInt16LE(0, 28);
    nameBuf.copy(localHeader, 30);

    const centralHeader = Buffer.alloc(46 + nameBuf.length);
    centralHeader.writeUInt32LE(0x02014b50, 0);
    centralHeader.writeUInt16LE(20, 4);
    centralHeader.writeUInt16LE(20, 6);
    centralHeader.writeUInt16LE(0, 8);
    centralHeader.writeUInt16LE(8, 10);
    centralHeader.writeUInt16LE(0, 12);
    centralHeader.writeUInt16LE(0x5d5c, 14);
    centralHeader.writeUInt32LE(crc, 16);
    centralHeader.writeUInt32LE(compressed.length, 20);
    centralHeader.writeUInt32LE(rawData.length, 24);
    centralHeader.writeUInt16LE(nameBuf.length, 28);
    centralHeader.writeUInt16LE(0, 30);
    centralHeader.writeUInt16LE(0, 32);
    centralHeader.writeUInt16LE(0, 34);
    centralHeader.writeUInt16LE(0, 36);
    centralHeader.writeUInt32LE(0, 38);
    centralHeader.writeUInt32LE(offset, 42);
    nameBuf.copy(centralHeader, 46);

    localHeaders.push(localHeader, compressed);
    centralHeaders.push(centralHeader);
    offset += localHeader.length + compressed.length;
  }

  const centralDirSize = centralHeaders.reduce((acc, b) => acc + b.length, 0);
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(0, 4);
  eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(entries.length, 8);
  eocd.writeUInt16LE(entries.length, 10);
  eocd.writeUInt32LE(centralDirSize, 12);
  eocd.writeUInt32LE(offset, 16);
  eocd.writeUInt16LE(0, 20);

  return Buffer.concat([...localHeaders, ...centralHeaders, eocd]);
}

const contentTypesXml = `<?xml version="1.0" encoding="utf-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension=".json" ContentType="application/json"/>
  <Default Extension=".js" ContentType="application/javascript"/>
  <Default Extension=".md" ContentType="text/markdown"/>
  <Default Extension=".png" ContentType="image/png"/>
  <Default Extension=".txt" ContentType="text/plain"/>
  <Default Extension=".vsixmanifest" ContentType="text/xml"/>
</Types>`;

const vsixManifestPath = path.join(extDir, '.vsixmanifest');
const vsixManifest = fs.readFileSync(vsixManifestPath, 'utf8')
  .replace(/<Identity\b[^>]*>/, `<Identity Language="pt-BR" Id="altimeter" Version="${targetVersion}" Publisher="${targetPublisher}" />`)
  .replace(/<Description\b[^>]*>[\s\S]*?<\/Description>/, `<Description xml:space="preserve">${targetDescription}</Description>`)
  .replace(/https:\/\/github\.com\/pqub\/altimeter/g, targetRepoUrl);
fs.writeFileSync(vsixManifestPath, vsixManifest, 'utf8');

const pkgJsonPath = path.join(extDir, 'package.json');
const pkgJsonObj = JSON.parse(fs.readFileSync(pkgJsonPath, 'utf8'));
pkgJsonObj.version = targetVersion;
pkgJsonObj.publisher = targetPublisher;
pkgJsonObj.author = targetAuthor;
pkgJsonObj.description = targetDescription;
pkgJsonObj.homepage = `${targetRepoUrl}#readme`;
pkgJsonObj.bugs = { url: `${targetRepoUrl}/issues` };
pkgJsonObj.repository = { type: 'git', url: `${targetRepoUrl}.git` };
fs.writeFileSync(pkgJsonPath, JSON.stringify(pkgJsonObj, null, '\t'), 'utf8');

const vsixPkgObj = { ...pkgJsonObj };
delete vsixPkgObj.__metadata;
const pkgJsonStr = JSON.stringify(vsixPkgObj, null, '\t');

const customIconPath = path.join(distBackupDir, 'icon.png');
const extIconPath = path.join(extDir, 'resources', 'icon.png');
if (fs.existsSync(customIconPath)) {
  fs.mkdirSync(path.dirname(extIconPath), { recursive: true });
  fs.copyFileSync(customIconPath, extIconPath);
  console.log('Ícone atualizado na IDE:', extIconPath);
}

const entries = [
  { name: '[Content_Types].xml', data: contentTypesXml },
  { name: 'extension.vsixmanifest', data: vsixManifest },
  { name: 'extension/package.json', data: pkgJsonStr },
  { name: 'extension/dist/extension.js', data: fs.readFileSync(sourceJsForVsix) }
];

const optionalFiles = [
  'readme.md',
  'changelog.md',
  'LICENSE.txt',
  'resources/icon.png',
  'resources/libs/chart.min.js'
];

for (const rel of optionalFiles) {
  const fullPath = path.join(extDir, rel);
  if (fs.existsSync(fullPath)) {
    entries.push({ name: 'extension/' + rel, data: fs.readFileSync(fullPath) });
  }
}

const vsixBuffer = createZip(entries);
const vsixPath = path.join(outDir, `altimeter-${targetVersion}-agy2.6-fix.vsix`);
fs.writeFileSync(vsixPath, vsixBuffer);
console.log('VSIX gerado com sucesso:', vsixPath, '(' + vsixBuffer.length + ' bytes)');

// Atualiza registro em extensions.json na IDE
const extensionsJsonPath = path.join(extensionsRoot, 'extensions.json');
if (fs.existsSync(extensionsJsonPath)) {
  try {
    const extList = JSON.parse(fs.readFileSync(extensionsJsonPath, 'utf8'));
    if (Array.isArray(extList)) {
      const normalizedExtDir = extDir.replace(/\\/g, '/');
      const uriPath = /^[A-Za-z]:/.test(normalizedExtDir)
        ? '/' + normalizedExtDir[0].toLowerCase() + normalizedExtDir.slice(1)
        : normalizedExtDir;
      let updated = false;
      const nextList = [];
      for (const item of extList) {
        const id = item && item.identifier && item.identifier.id;
        if (id === 'pqub.altimeter' || id === `${targetPublisher}.altimeter`) {
          if (updated) continue;
          item.identifier.id = `${targetPublisher}.altimeter`;
          delete item.identifier.uuid;
          item.version = targetVersion;
          item.relativeLocation = targetFolderName;
          const externalUri = 'file:///' + uriPath.replace(/^\//, '').replace(':', '%3A');
          item.location = {
            $mid: 1,
            fsPath: extDir,
            _sep: 1,
            external: externalUri,
            path: uriPath,
            scheme: 'file'
          };
          item.metadata = Object.assign({}, item.metadata || {}, {
            publisherDisplayName: targetPublisher,
            installedTimestamp: Date.now(),
            source: 'vsix'
          });
          delete item.metadata.id;
          delete item.metadata.publisherId;
          updated = true;
        }
        nextList.push(item);
      }
      if (updated) {
        fs.writeFileSync(extensionsJsonPath, JSON.stringify(nextList), 'utf8');
        console.log('Registro atualizado em:', extensionsJsonPath);
      }
    }
  } catch (err) {
    console.warn('Aviso ao atualizar extensions.json:', err.message);
  }
}

// Remove pastas antigas da extensao para evitar conflitos de comandos na IDE
for (const name of fs.readdirSync(extensionsRoot)) {
  const isLegacy = name.startsWith('pqub.altimeter-');
  const isOldOwn = name.startsWith(`${targetPublisher}.altimeter-`) && name !== targetFolderName;
  if (isLegacy || isOldOwn) {
    const oldPath = path.join(extensionsRoot, name);
    fs.rmSync(oldPath, { recursive: true, force: true });
    console.log('Pasta antiga removida:', oldPath);
  }
}

// Move o pacote .vsix antigo (0.0.6) para backup_altimeter_0.0.4/ caso exista na raiz
const oldVsixPath = path.join(outDir, 'altimeter-0.0.6-agy2.6-fix.vsix');
if (fs.existsSync(oldVsixPath)) {
  const backupDir = path.join(outDir, 'backup_altimeter_0.0.4');
  fs.mkdirSync(backupDir, { recursive: true });
  const backupOldVsixPath = path.join(backupDir, 'altimeter-0.0.6-agy2.6-fix.vsix');
  fs.renameSync(oldVsixPath, backupOldVsixPath);
  console.log('Pacote anterior movido para backup:', backupOldVsixPath);
}

