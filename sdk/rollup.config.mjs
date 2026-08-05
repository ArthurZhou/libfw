/**
 * Rollup config that bundles the ESM SDK + the wasm-bindgen `web` glue into
 * a single UMD file (used via a `<script>` tag, no bundler required).
 *
 * Build: `npm run build:umd` (after `npm run build:wasm`).
 */
import { nodeResolve } from '@rollup/plugin-node-resolve';

export default {
  input: 'index.js',
  output: {
    file: 'dist/libfw-client.umd.js',
    format: 'umd',
    name: 'LibfwClient',
    exports: 'named',
    sourcemap: true,
  },
  plugins: [nodeResolve()],
};
