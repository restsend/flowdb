/**
 * Generate platform-specific npm packages from built .node artifacts.
 *
 * Usage: node scripts/publish-platforms.js
 *
 * Expects:
 *   - artifacts/ dir with *.node files (e.g., flowdb-node.darwin-arm64.node)
 *   - package.json with napi.binaryName and napi.targets
 *
 * Output:
 *   - npm/{platform}/package.json + {binaryName}.{platform}.node
 */

const fs = require('fs')
const path = require('path')

const ROOT = path.resolve(__dirname, '..')
const PKG = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'))
const NAME = PKG.napi?.binaryName || 'flowdb-node'
const VERSION = PKG.version

// ── Platform definitions ──────────────────────────────────────────
// Maps artifact suffix → npm package config
const PLATFORMS = {
  'darwin-arm64':   { os: ['darwin'], cpu: ['arm64'] },
  'darwin-x64':     { os: ['darwin'], cpu: ['x64'] },
  'linux-x64-gnu':  { os: ['linux'],  cpu: ['x64'],  libc: ['glibc'] },
  'linux-arm64-gnu':{ os: ['linux'],  cpu: ['arm64'],libc: ['glibc'] },
  'win32-x64-msvc': { os: ['win32'],  cpu: ['x64'] },
}

function main() {
  const artifacts = fs.readdirSync(path.join(ROOT, 'artifacts'))
    .filter(f => f.endsWith('.node'))

  if (artifacts.length === 0) {
    console.error('No .node files found in artifacts/')
    process.exit(1)
  }

  for (const file of artifacts) {
    // Extract platform suffix from filename: flowdb-node.darwin-arm64.node
    const match = file.match(new RegExp(`^${NAME}\\.(.+)\\.node$`))
    if (!match) {
      console.warn('Skipping unrecognized artifact:', file)
      continue
    }
    const suffix = match[1]
    const plat = PLATFORMS[suffix]
    if (!plat) {
      console.warn('Skipping unknown platform:', suffix)
      continue
    }

    const pkgDir = path.join(ROOT, 'npm', suffix)
    fs.mkdirSync(pkgDir, { recursive: true })

    // Copy .node file
    fs.copyFileSync(
      path.join(ROOT, 'artifacts', file),
      path.join(pkgDir, file),
    )

    // Generate package.json
    const platformPkg = {
      name: `@restsend/flowdb-${suffix}`,
      version: VERSION,
      description: `FlowDB native binary for ${suffix}`,
      ...plat,
      main: file,
    }
    fs.writeFileSync(
      path.join(pkgDir, 'package.json'),
      JSON.stringify(platformPkg, null, 2) + '\n',
    )

    console.log(`✓ Generated npm/${suffix}/`)
  }

  console.log(`\nDone. ${artifacts.length} platform package(s) ready in npm/`)
}

main()
