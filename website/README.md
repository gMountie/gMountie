# gMountie docs site

The [Docusaurus](https://docusaurus.io/) app that builds **docs.gmountie.dev**. It
replaced the previous docsify setup.

## Where the content lives

Markdown lives in **`../docs`** (this app points its `docs` plugin at it), not in
`website/`. That keeps the `.md` files rendering on GitHub at their familiar paths
and lets the docs version with the code in the same PR.

- **Author in plain `.md`.** `markdown.format: 'detect'` parses `.md` as CommonMark —
  portable and GitHub-renderable. Stick to plain Markdown / GFM so a page reads the
  same on GitHub and on the site.
- **`.mdx` is the escape hatch.** Rename a page to `.mdx` only when it genuinely needs
  React components or admonition/tab sugar. MDX does **not** render on GitHub, so use
  it sparingly.
- `docs/README.md` is a symlink to the repo-root README and is excluded from the build;
  the site home is `docs/index.md`.
- `docs/superpowers/**` (transient planning artifacts) is excluded from the build.

## Local development

```bash
npm install          # first time
npm start            # dev server with live reload
npm run build        # production build into website/build
npm run serve        # serve the production build locally
```

## Versioning

Versioning is configured and ready; no version has been cut yet, so the site serves
only the current ("next") docs and the navbar version dropdown stays hidden.

Cut a version at release time, snapshotting the current `../docs` into
`website/versioned_docs/`:

```bash
npm run docusaurus docs:version 0.9
```

This copies the docs as they are **right now**, so cut the version from the commit
whose docs match the release (e.g. check out the release tag, or cut on `master`
immediately before tagging). Keep the number of live versions modest (<10).

## Deployment

CI builds and deploys via `.github/workflows/docs.yml` (on `release: published`, or
manually via `workflow_dispatch`). `static/CNAME` carries the `docs.gmountie.dev`
custom domain into the build output. No manual `npm run deploy` is needed.

## Brand

The theme re-skins Infima with the "Bonded Courier" design system tokens
(`src/css/custom.css`): ember accent, paper/ink surfaces, Newsreader + Geist. Logo,
favicon, and wordmark in `static/img/` come from `gMountie-brand/brand/design-system/`.
