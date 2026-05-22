# Translations

The translation files in this directory are AI-generated and provided as a
courtesy.  They represent a best-effort attempt at localisation and do not
strive for complete correctness, grammatical precision, or cultural
appropriateness in every language.

We welcome additions and pull requests, but make no guarantees about the
quality, accuracy, or completeness of any translation.  Contributions may
incidentally be overwritten by future AI-assisted updates.

If you notice an error or want to improve a translation, please open a pull
request.  Native-speaker contributions are especially appreciated.

## Adding a new language

1. Copy `en.ftl` as a starting point and rename it to `<BCP-47-code>.ftl`
   (e.g. `sv.ftl`, `zh-Hans.ftl`, `art-x-lyaric.ftl`).
2. Translate every `key = value` line.  Keep the key names unchanged.
3. Include a `lang-name = <autonym>` line — the language's own name for
   itself (e.g. `lang-name = Svenska`).
4. Rebuild (`cargo build`).  The build script picks up the new file
   automatically — no code changes needed.
