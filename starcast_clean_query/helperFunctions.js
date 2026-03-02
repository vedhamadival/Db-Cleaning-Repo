// Normalize and clean phone numbers
function normalizePhone(raw) {
  if (!raw) return null;
  const lower = String(raw).toLowerCase();
  if (lower.includes("deleted") || lower.includes("nulldeleted")) return null;

  let clean = lower.replace(/[\s\-()]/g, "");
  if (clean.startsWith("+")) clean = clean.slice(1);
  if (clean.startsWith("00")) clean = clean.replace(/^00+/, "");
  clean = clean.replace(/[^0-9]/g, "");
  if (clean.length >= 10 && clean.length <= 15) return clean;
  return null;
}

// Clean and validate email — reject corrupted or empty
function cleanEmail(raw) {
  if (!raw) return null;
  const v = String(raw).trim();
  if (v.length === 0) return null;
  const lower = v.toLowerCase();
  // Reject obvious deleted tokens
  if (lower.includes("deleted") || lower.includes("nulldeleted")) return null;

  // Basic email validation: local@domain.tld (keeps many valid but filters broken strings)
  // This is intentionally conservative and avoids heavy RFC compliance for simplicity.
  const simpleEmailRe = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!simpleEmailRe.test(lower)) {
    // If it looks like emails are wrapped in brackets or have noise, try to extract an email-like token
    const extractRe = /([a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})/i;
    const m = v.match(extractRe);
    if (m) return m[1].toLowerCase();
    return null;
  }

  return lower;
}

// Clean name fields — lowercase only for check, keep original case for storage
function cleanName(raw) {
  if (!raw) return null;
  const s = String(raw).trim();
  if (s.length === 0) return null;
  const lower = s.toLowerCase();
  if (lower.includes("deleted") || lower.includes("nulldeleted")) return null;

  // If the name doesn't contain any alphabetic characters, treat it as missing
  if (!/[A-Za-z]/.test(s)) return null;

  return s;
}

function cleanGender(raw) {
  if (!raw) return null;
  const lower = raw.toLowerCase();
  if (lower.includes("deleted") || lower.includes("nulldeleted")) return null;

  const s = raw.trim();
  return s.length > 0 ? s : null;
}



// Parse birthdate from various noisy formats and return YYYY-MM-DD or null
function parseBirthdate(raw) {
  if (!raw) return null;
  // Accept Date objects directly
  if (raw instanceof Date) {
    if (!isNaN(raw)) return raw.toISOString().split("T")[0];
    return null;
  }

  let s = String(raw).trim();
  if (s.length === 0) return null;

  // Remove common noise: trailing ".0", and era markers like AD/BC
  s = s.replace(/\.\d+$/, "");
  s = s.replace(/\s*(AD|BC)$/i, "");

  // Try direct Date parse
  const d = new Date(s);
  if (!isNaN(d)) return d.toISOString().split("T")[0];

  // Extract ISO-like YYYY-MM-DD substring
  const m1 = s.match(/(\d{4}-\d{2}-\d{2})/);
  if (m1) return m1[1];

  // Extract YYYY/MM/DD and convert
  const m2 = s.match(/(\d{4}\/\d{2}\/\d{2})/);
  if (m2) return m2[1].replace(/\//g, "-");

  // Fallback: capture three numeric groups (YYYY non-greedy sep MM sep DD)
  const m3 = s.match(/(\d{4})\D+(\d{1,2})\D+(\d{1,2})/);
  if (m3) {
    const y = m3[1];
    const mo = String(m3[2]).padStart(2, "0");
    const da = String(m3[3]).padStart(2, "0");
    return `${y}-${mo}-${da}`;
  }

  return null;
}

// Format timestamp to "MM/DD/YYYY HH:MM"
function formatTimestamp(date) {
  if (!date) return null;
  
  // Handle "AD" suffix common in some Postgres exports
  if (typeof date === 'string') {
    date = date.replace(/\s*AD$/i, '').trim();
  }

  const d = new Date(date);
  if (isNaN(d.getTime())) return null;

  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const yyyy = d.getFullYear();
  const hh = String(d.getHours()).padStart(2, '0');
  const min = String(d.getMinutes()).padStart(2, '0');

  return `${mm}/${dd}/${yyyy} ${hh}:${min}`;
}


function cleanDesignation(raw) {
  if (!raw) return null;
  const s = String(raw).trim();
  if (s.length === 0) return null;

  const lower = s.toLowerCase();
  if (lower.includes("deleted") || lower.includes("nulldeleted")) return null;

  // Exclusion keywords - if present, do not consider this an actor designation
  const exclude = ["agent", "casting director", "casting-director", "casting_director", "cd"];
  for (const ex of exclude) {
    // use word boundary check for short tokens like 'cd' and 'agent'
    const re = new RegExp("\\b" + ex.replace(/[-\\/\\^$*+?.()|[\]{}]/g, "\\$&") + "\\b", "i");
    if (re.test(lower)) return null;
  }

  // Accept if 'actor' or 'actors' appears as a word
  const actorRe = /\bactors?\b/i;
  if (actorRe.test(lower)) return s; // return original trimmed value (preserve case)

  return null;
}

module.exports = {normalizePhone,cleanDesignation,parseBirthdate,cleanEmail,cleanGender,cleanName,formatTimestamp};