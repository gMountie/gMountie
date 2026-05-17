# gMountie — Brand Design Language (Phase 1)

This folder is the Phase-1 deliverable: a complete design language, not the final identity system or the mascot artwork (those are Phase 2 and Phase 3).

Open the files directly in a browser; there is no build step.

- **`index.html`** — landing page that demonstrates the design language in action
- **`style-guide.html`** — full brand book: tokens, type, color, voice, wordmark, monogram, CLI banner, illustration direction, logo lockups
- **`tokens.css`** — the shared design tokens used by both pages

## The aesthetic direction in one sentence

**Cartographer's Field Journal** — early-20th-century alpine survey: warm cream paper, deep iron-gall ink, a single ember/amber accent for the morning sun on alpine peaks, characterful serif paired with technical sans and precise mono.

## Decisions made

### Typography
- **Display:** Fraunces (variable; opsz, SOFT, wght axes) — characterful serif with literary weight, geographic-society feel
- **Sans:** IBM Plex Sans — technical credibility with humanist warmth, distinctive without being weird
- **Mono:** IBM Plex Mono — pairs with Sans, characterful for code and CLI

All three are free on Google Fonts and loaded from one stylesheet.

### Color palette
- **Surfaces (paper):** five-step ramp from cream `#F4ECD8` to grain `#C9B98C`
- **Foreground (ink):** four-step ramp from deep iron-ink `#1E1812` down through warm browns to `#B6A990`
- **Primary accent (Ember):** `#C5722A` — the single bold color, used for the lowercase `g`, primary CTAs, contour gestures, and one focal element per icon
- **Secondary accents:** Moss `#5E7045` (success), Lake `#3C5A6E` (info/links), Terracotta `#A8442F` (danger only — muted to avoid RCMP-red collision)
- Dark mode is real and on equal footing (see `style-guide.html` toggle)

### Voice & tone
Three locked rules:
1. **Numbers beat adjectives.** "Cold readdir in 12ms" replaces "Lightning fast."
2. **Earnest, never cute for cute's sake.** Maximum one emoji per page; none in CLI output.
3. **Speak about the tool, not the team.** Save first-person plural for release notes.

### Wordmark
- Casing is **LOCKED** at `gMountie` — lowercase g, capital M. The g is for **gRPC**.
- Set in Fraunces (opsz 96, SOFT 30, wght 380, tracking -0.035em)
- The lowercase `g` is rendered in Ember — the brand's signature note
- A faint topographic contour line continues from the wordmark to the right; the same line weight recurs in section rules and the architecture diagram

### Monogram
- A broad-brimmed campaign hat in silhouette with one ember band on the brim
- Works at 128, 64, 32, and 16 px (the favicon shipped in both HTML files is the 16px version)
- Distinct from a fedora, a cowboy hat, or any RCMP iconography

### Iconography
- 24×24 grid, 1.5px stroke, rounded caps/joins, line-only by default
- One ember-filled element per icon (the focal point)
- Maximum three elements per icon — they label, not illustrate

### CLI presence
- Box-drawing characters survive monospace cleanly
- Single ember (terminal yellow/orange) tint when colors are available
- Pure monochrome fallback when `NO_COLOR` is set
- Never animates beyond a two-frame spinner

### Illustration direction (mascot is Phase 3)
- **Species:** alpine **marmot** (not chipmunk, not beaver) — steady, calm, native to the terrain the brand evokes
- **Uniform:** broad-brimmed felt campaign hat with one ember band + a leather saddlebag carrying a folded page. **Never RCMP red serge.**
- **Line:** hand-drawn, scanned in, slightly imperfect — not vector-flat
- **Palette:** stays in the three-color world — paper, ink, one ember accent
- **Posture:** always in the act of doing something. Never centered, never smiling at the camera

## What's locked vs. what's open

**Locked (do not revisit without strong evidence):**
- Aesthetic direction (Cartographer's Field Journal)
- The three-font system (Fraunces / Plex Sans / Plex Mono)
- Primary accent (Ember `#C5722A`) and the rule of one bold color
- Wordmark casing (`gMountie`) and the g-is-for-gRPC story
- Monogram shape (campaign hat, ember band)
- Voice rules (numbers > adjectives, earnest > cute, tool > team)
- Mascot species: marmot
- No RCMP red

**Open for Phase 2:**
- Final wordmark glyph refinement (custom path for the `g` descender)
- Full icon set (Phase 1 demonstrates ~8 icons; Phase 2 ships the full library)
- Asset exports (SVG, PNG variants, ICO, OG images)
- Token export to other formats (Tailwind, Style Dictionary)
- Component library (buttons, cards, forms, tables)

**Open for Phase 3:**
- The actual marmot illustration (sketch placeholder is in the style guide; final artwork goes here)
- Animated states (no animation in Phase 1)
- Merchandise / community / event assets

## How to extend the system

The single source of truth for tokens is `tokens.css`. To use this system in another surface (the existing README, future docs site, marketing pages, the Wails UI):

1. Import the same Google Fonts stylesheet
2. Copy the CSS custom properties from `tokens.css` into your scope
3. Use the same class patterns (`.wordmark`, `.eyebrow`, `.lede`, `.field-rule`, `.tag`, `.btn--primary`, `.btn--ghost`)

The point of the system is that every surface looks like it came from the same hand. If you find yourself reaching for a new color, ask whether one of the existing tokens would do the job. The answer is almost always yes.

— Phase 1 of III · the courier is in the post.
