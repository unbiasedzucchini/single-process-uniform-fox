#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const usage = `Usage:
  node main.js <cas_dir> write -           Write stdin to CAS
  node main.js <cas_dir> write <file>      Write file to CAS
  node main.js <cas_dir> read <name>       Read from CAS to stdout

Edit operations (read from CAS, modify, write back, output new name):
  node main.js <cas_dir> replace <name> <old> <new>       Replace first occurrence
  node main.js <cas_dir> replace-all <name> <old> <new>   Replace all occurrences
  node main.js <cas_dir> line-insert <name> <n> <text>    Insert line at position n (1-indexed)
  node main.js <cas_dir> line-delete <name> <n>           Delete line n (1-indexed)
  node main.js <cas_dir> line-replace <name> <n> <text>   Replace line n (1-indexed)
  node main.js <cas_dir> append <name> <text>             Append text to end
  node main.js <cas_dir> prepend <name> <text>            Prepend text to beginning`;

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

function readCAS(casDir, name) {
  const filePath = getFilePath(casDir, name);

  if (!fs.existsSync(filePath)) {
    console.error(`Error: ${name} not found in CAS`);
    process.exit(1);
  }

  return fs.readFileSync(filePath);
}

function read(casDir, name) {
  const data = readCAS(casDir, name);
  process.stdout.write(data);
}

function writeCAS(casDir, data) {
  const hash = computeHash(data);
  const filePath = getFilePath(casDir, hash);
  const dir = path.dirname(filePath);

  fs.mkdirSync(dir, { recursive: true });

  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, data);
  }

  return hash;
}

// Edit operation helper: read, transform, write, output new hash
function edit(casDir, name, transformFn) {
  const data = readCAS(casDir, name);
  const text = data.toString('utf8');
  const newText = transformFn(text);
  const newHash = writeCAS(casDir, Buffer.from(newText, 'utf8'));
  console.log(newHash);
}

// Text/Line editing primitives
function replace(casDir, name, oldStr, newStr) {
  edit(casDir, name, text => text.replace(oldStr, newStr));
}

function replaceAll(casDir, name, oldStr, newStr) {
  edit(casDir, name, text => text.split(oldStr).join(newStr));
}

function lineInsert(casDir, name, lineNum, newLine) {
  edit(casDir, name, text => {
    const lines = text.split('\n');
    const idx = lineNum - 1; // Convert to 0-indexed
    if (idx < 0 || idx > lines.length) {
      console.error(`Error: line ${lineNum} out of range (1-${lines.length + 1})`);
      process.exit(1);
    }
    lines.splice(idx, 0, newLine);
    return lines.join('\n');
  });
}

function lineDelete(casDir, name, lineNum) {
  edit(casDir, name, text => {
    const lines = text.split('\n');
    const idx = lineNum - 1;
    if (idx < 0 || idx >= lines.length) {
      console.error(`Error: line ${lineNum} out of range (1-${lines.length})`);
      process.exit(1);
    }
    lines.splice(idx, 1);
    return lines.join('\n');
  });
}

function lineReplace(casDir, name, lineNum, newLine) {
  edit(casDir, name, text => {
    const lines = text.split('\n');
    const idx = lineNum - 1;
    if (idx < 0 || idx >= lines.length) {
      console.error(`Error: line ${lineNum} out of range (1-${lines.length})`);
      process.exit(1);
    }
    lines[idx] = newLine;
    return lines.join('\n');
  });
}

function append(casDir, name, text) {
  edit(casDir, name, content => content + text);
}

function prepend(casDir, name, text) {
  edit(casDir, name, content => text + content);
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

    case 'replace':
      if (rest.length !== 3) {
        console.error('replace requires: <name> <old> <new>');
        process.exit(1);
      }
      replace(casDir, rest[0], rest[1], rest[2]);
      break;

    case 'replace-all':
      if (rest.length !== 3) {
        console.error('replace-all requires: <name> <old> <new>');
        process.exit(1);
      }
      replaceAll(casDir, rest[0], rest[1], rest[2]);
      break;

    case 'line-insert':
      if (rest.length !== 3) {
        console.error('line-insert requires: <name> <line-num> <text>');
        process.exit(1);
      }
      lineInsert(casDir, rest[0], parseInt(rest[1], 10), rest[2]);
      break;

    case 'line-delete':
      if (rest.length !== 2) {
        console.error('line-delete requires: <name> <line-num>');
        process.exit(1);
      }
      lineDelete(casDir, rest[0], parseInt(rest[1], 10));
      break;

    case 'line-replace':
      if (rest.length !== 3) {
        console.error('line-replace requires: <name> <line-num> <text>');
        process.exit(1);
      }
      lineReplace(casDir, rest[0], parseInt(rest[1], 10), rest[2]);
      break;

    case 'append':
      if (rest.length !== 2) {
        console.error('append requires: <name> <text>');
        process.exit(1);
      }
      append(casDir, rest[0], rest[1]);
      break;

    case 'prepend':
      if (rest.length !== 2) {
        console.error('prepend requires: <name> <text>');
        process.exit(1);
      }
      prepend(casDir, rest[0], rest[1]);
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
