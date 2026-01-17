#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { DatabaseSync } = require('node:sqlite');

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
  node main.js <cas_dir> prepend <name> <text>            Prepend text to beginning

LLM operations:
  node main.js <cas_dir> llm <model> <system> <prompt>    Call LLM, tee output to CAS and stdout`;

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

// Audit log database (lives in _audit.db to avoid collision with CAS content)
let auditDb = null;

function initAuditDb(casDir) {
  fs.mkdirSync(casDir, { recursive: true });
  const dbPath = path.join(casDir, '_audit.db');
  auditDb = new DatabaseSync(dbPath);
  auditDb.exec(`
    CREATE TABLE IF NOT EXISTS audit_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp TEXT NOT NULL,
      command TEXT NOT NULL,
      arguments TEXT NOT NULL,
      success INTEGER NOT NULL,
      error_message TEXT,
      output_name TEXT
    )
  `);
}

function logAudit(command, args, success, errorMessage = null, outputName = null) {
  if (!auditDb) return;
  const stmt = auditDb.prepare(`
    INSERT INTO audit_log (timestamp, command, arguments, success, error_message, output_name)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  stmt.run(
    new Date().toISOString(),
    command,
    JSON.stringify(args),
    success ? 1 : 0,
    errorMessage,
    outputName
  );
}

async function write(casDir, source) {
  let data;
  if (source === '-') {
    data = await readStdin();
  } else {
    data = fs.readFileSync(source);
  }

  const hash = writeCAS(casDir, data);
  console.log(hash);
  return hash;
}

function readCAS(casDir, name) {
  const filePath = getFilePath(casDir, name);

  if (!fs.existsSync(filePath)) {
    throw new Error(`${name} not found in CAS`);
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

// Edit operation helper: read, transform, write, return new hash
function edit(casDir, name, transformFn) {
  const data = readCAS(casDir, name);
  const text = data.toString('utf8');
  const newText = transformFn(text);
  const newHash = writeCAS(casDir, Buffer.from(newText, 'utf8'));
  console.log(newHash);
  return newHash;
}

// Text/Line editing primitives
function replace(casDir, name, oldStr, newStr) {
  return edit(casDir, name, text => text.replace(oldStr, newStr));
}

function replaceAll(casDir, name, oldStr, newStr) {
  return edit(casDir, name, text => text.split(oldStr).join(newStr));
}

function lineInsert(casDir, name, lineNum, newLine) {
  return edit(casDir, name, text => {
    const lines = text.split('\n');
    const idx = lineNum - 1; // Convert to 0-indexed
    if (idx < 0 || idx > lines.length) {
      throw new Error(`line ${lineNum} out of range (1-${lines.length + 1})`);
    }
    lines.splice(idx, 0, newLine);
    return lines.join('\n');
  });
}

function lineDelete(casDir, name, lineNum) {
  return edit(casDir, name, text => {
    const lines = text.split('\n');
    const idx = lineNum - 1;
    if (idx < 0 || idx >= lines.length) {
      throw new Error(`line ${lineNum} out of range (1-${lines.length})`);
    }
    lines.splice(idx, 1);
    return lines.join('\n');
  });
}

function lineReplace(casDir, name, lineNum, newLine) {
  return edit(casDir, name, text => {
    const lines = text.split('\n');
    const idx = lineNum - 1;
    if (idx < 0 || idx >= lines.length) {
      throw new Error(`line ${lineNum} out of range (1-${lines.length})`);
    }
    lines[idx] = newLine;
    return lines.join('\n');
  });
}

function append(casDir, name, text) {
  return edit(casDir, name, content => content + text);
}

function prepend(casDir, name, text) {
  return edit(casDir, name, content => text + content);
}

// LLM operation via OpenRouter
async function llm(casDir, model, system, prompt) {
  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) {
    throw new Error('OPENROUTER_API_KEY environment variable not set');
  }

  const response = await fetch('https://openrouter.ai/api/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: model,
      stream: true,
      messages: [
        { role: 'system', content: system },
        { role: 'user', content: prompt },
      ],
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`OpenRouter API error: ${response.status} ${text}`);
  }

  // Stream the response, collecting output
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let fullOutput = '';
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop(); // Keep incomplete line in buffer

    for (const line of lines) {
      if (line.startsWith('data: ')) {
        const data = line.slice(6);
        if (data === '[DONE]') continue;
        try {
          const parsed = JSON.parse(data);
          const content = parsed.choices?.[0]?.delta?.content;
          if (content) {
            process.stdout.write(content);
            fullOutput += content;
          }
        } catch (e) {
          // Skip malformed JSON
        }
      }
    }
  }

  // Ensure newline at end of output
  if (fullOutput && !fullOutput.endsWith('\n')) {
    process.stdout.write('\n');
  }

  // Write to CAS and return hash
  const hash = writeCAS(casDir, Buffer.from(fullOutput, 'utf8'));
  console.error(`CAS: ${hash}`);
  return hash;
}

async function runCommand(casDir, command, rest) {
  switch (command) {
    case 'write':
      if (rest.length !== 1) {
        throw new Error('write requires exactly one argument (- or file path)');
      }
      return await write(casDir, rest[0]);

    case 'read':
      if (rest.length !== 1) {
        throw new Error('read requires exactly one argument (hash name)');
      }
      read(casDir, rest[0]);
      return null; // read outputs to stdout, no CAS output name

    case 'replace':
      if (rest.length !== 3) {
        throw new Error('replace requires: <name> <old> <new>');
      }
      return replace(casDir, rest[0], rest[1], rest[2]);

    case 'replace-all':
      if (rest.length !== 3) {
        throw new Error('replace-all requires: <name> <old> <new>');
      }
      return replaceAll(casDir, rest[0], rest[1], rest[2]);

    case 'line-insert':
      if (rest.length !== 3) {
        throw new Error('line-insert requires: <name> <line-num> <text>');
      }
      return lineInsert(casDir, rest[0], parseInt(rest[1], 10), rest[2]);

    case 'line-delete':
      if (rest.length !== 2) {
        throw new Error('line-delete requires: <name> <line-num>');
      }
      return lineDelete(casDir, rest[0], parseInt(rest[1], 10));

    case 'line-replace':
      if (rest.length !== 3) {
        throw new Error('line-replace requires: <name> <line-num> <text>');
      }
      return lineReplace(casDir, rest[0], parseInt(rest[1], 10), rest[2]);

    case 'append':
      if (rest.length !== 2) {
        throw new Error('append requires: <name> <text>');
      }
      return append(casDir, rest[0], rest[1]);

    case 'prepend':
      if (rest.length !== 2) {
        throw new Error('prepend requires: <name> <text>');
      }
      return prepend(casDir, rest[0], rest[1]);

    case 'llm':
      if (rest.length !== 3) {
        throw new Error('llm requires: <model> <system> <prompt>');
      }
      return await llm(casDir, rest[0], rest[1], rest[2]);

    default:
      throw new Error(`Unknown command: ${command}`);
  }
}

async function main() {
  const args = process.argv.slice(2);

  if (args.length < 2) {
    console.error(usage);
    process.exit(1);
  }

  const [casDir, command, ...rest] = args;

  // Initialize audit database
  initAuditDb(casDir);

  try {
    const outputName = await runCommand(casDir, command, rest);
    logAudit(command, rest, true, null, outputName);
  } catch (err) {
    logAudit(command, rest, false, err.message, null);
    console.error(`Error: ${err.message}`);
    process.exit(1);
  }
}

main();
