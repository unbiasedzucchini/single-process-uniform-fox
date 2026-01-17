#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const usage = `Usage:
  node main.js <cas_dir> write -           Write stdin to CAS
  node main.js <cas_dir> write <file>      Write file to CAS
  node main.js <cas_dir> read <name>       Read from CAS to stdout`;

async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function computeHash(data) {
  return crypto.createHash('sha256').update(data).digest('hex');
}

function getFilePath(casDir, hash) {
  // Use first 2 chars as subdirectory for better filesystem performance
  const subdir = hash.slice(0, 2);
  const filename = hash.slice(2);
  return path.join(casDir, subdir, filename);
}

async function write(casDir, source) {
  let data;
  if (source === '-') {
    data = await readStdin();
  } else {
    data = fs.readFileSync(source);
  }

  const hash = computeHash(data);
  const filePath = getFilePath(casDir, hash);
  const dir = path.dirname(filePath);

  // Create directory if needed
  fs.mkdirSync(dir, { recursive: true });

  // Only write if not already present (content-addressable = immutable)
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, data);
  }

  console.log(hash);
}

function read(casDir, name) {
  const filePath = getFilePath(casDir, name);

  if (!fs.existsSync(filePath)) {
    console.error(`Error: ${name} not found in CAS`);
    process.exit(1);
  }

  const data = fs.readFileSync(filePath);
  process.stdout.write(data);
}

async function main() {
  const args = process.argv.slice(2);

  if (args.length < 2) {
    console.error(usage);
    process.exit(1);
  }

  const [casDir, command, ...rest] = args;

  switch (command) {
    case 'write':
      if (rest.length !== 1) {
        console.error('write requires exactly one argument (- or file path)');
        process.exit(1);
      }
      await write(casDir, rest[0]);
      break;

    case 'read':
      if (rest.length !== 1) {
        console.error('read requires exactly one argument (hash name)');
        process.exit(1);
      }
      read(casDir, rest[0]);
      break;

    default:
      console.error(`Unknown command: ${command}`);
      console.error(usage);
      process.exit(1);
  }
}

main().catch(err => {
  console.error(err.message);
  process.exit(1);
});
