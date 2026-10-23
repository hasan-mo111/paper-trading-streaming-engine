#!/usr/bin/env node
/* =============================================================================
 *  Generate an ADMIN_PASSWORD_HASH for the Render environment.
 *
 *    npm run hash:admin -- "your long password"
 *
 *  Output: scrypt$<salt-b64url>$<hash-b64url>
 *  Put that whole string in ADMIN_PASSWORD_HASH and remove ADMIN_PASSWORD.
 * ============================================================================= */

import { hashPassword } from '../src/middleware/adminAuth.js';

const password = process.argv[2];

if (!password) {
  console.error('\nUsage: npm run hash:admin -- "your password"\n');
  process.exit(1);
}

if (password.length < 10) {
  console.error('\nRefusing to hash a short password: use at least 10 characters.\n');
  process.exit(1);
}

console.log('\nADMIN_PASSWORD_HASH=' + hashPassword(password) + '\n');
console.log('Store the value above in Render → Environment, then redeploy.');
console.log('Keep ADMIN_SESSION_SECRET separate so rotating the password does not');
console.log('invalidate sessions for the wrong reason.\n');
