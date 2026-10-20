// Dependency-free internal link/anchor checker for the built site (dist/).
// Usage: node scripts/check-links.mjs [distDir]   (run after `astro build`)
import { readdirSync, readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve, relative, posix } from 'node:path';

const dist = resolve(process.argv[2] ?? 'dist');
const BASE = '/scriva/';

function walk(dir) {
  return readdirSync(dir).flatMap((n) => {
    const p = join(dir, n);
    return statSync(p).isDirectory() ? walk(p) : [p];
  });
}

const pages = walk(dist).filter((f) => f.endsWith('.html'));
const idsByPage = new Map();
for (const f of pages) {
  const html = readFileSync(f, 'utf8');
  idsByPage.set(f, new Set([...html.matchAll(/\sid=["']([^"']+)["']/g)].map((m) => m[1])));
}

// Resolve a URL path (already base-stripped, no hash) to a file in dist.
function target(urlPath) {
  const p = join(dist, decodeURIComponent(urlPath));
  for (const c of [p, join(p, 'index.html'), p + '.html']) {
    if (existsSync(c) && statSync(c).isFile()) return c;
  }
  return null;
}

const errors = [];
let checked = 0;
for (const f of pages) {
  const html = readFileSync(f, 'utf8');
  const pageUrl = BASE + relative(dist, f).replace(/index\.html$/, '').split('\\').join('/');
  for (const m of html.matchAll(/<a\s[^>]*?href=["']([^"']+)["']/g)) {
    const href = m[1].replace(/&amp;/g, '&');
    if (/^(https?:|mailto:|tel:|javascript:|data:)/i.test(href) || href.startsWith('//')) continue;
    checked++;
    const [pathPart, hash] = href.split('#');
    const query = pathPart.split('?')[0];
    let abs = query ? (query.startsWith('/') ? query : posix.join(dirname(pageUrl + 'x'), query)) : pageUrl;
    if (!abs.startsWith(BASE)) {
      if (query.startsWith('/')) errors.push(`${relative(dist, f)}: link outside base path: ${href}`);
      continue;
    }
    const file = target(abs.slice(BASE.length));
    if (!file) { errors.push(`${relative(dist, f)}: broken link: ${href}`); continue; }
    if (hash && file.endsWith('.html') && !idsByPage.get(file)?.has(decodeURIComponent(hash))) {
      errors.push(`${relative(dist, f)}: missing anchor: ${href}`);
    }
  }
}

if (errors.length) {
  console.error(errors.join('\n'));
  console.error(`\n${errors.length} broken link(s) in ${checked} checked`);
  process.exit(1);
}
console.log(`link check OK: ${checked} internal links across ${pages.length} pages`);
