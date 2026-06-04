# Git history milestones

`main` is a short stack of milestone snapshots after `initial commit`:

| Milestone | Subject line | Scope |
|-----------|--------------|--------|
| Foundation | `foundation:` | Open source release, scraper core, CMS ingest baseline |
| Web | `web:` | Search (FTS), claim-rate letter, patient UI, themes |
| Pipeline | `pipeline:` | Hot DB shard, CI workflows, audit pipeline, dataset verify |
| Discovery | `data: discovery,` | Discovery inputs, URL enrichment, shard merges, build scripts |
| CI / index | `ci:` | 50-state matrix, MRF URL index harvest, health-system roots |
| Ingest / merge | `data: index matching,` | Index matching, parallel ingest, incremental merge, publish path |
| Coverage / ops | `data: coverage growth,` | Coverage growth, ROI pipeline, disk cleanup, stabilization |

Pre-reorganization history is preserved on `backup-main-*` branches.

To rebuild this layout from an older linear history:

```bash
./scripts/reorganize_git_history.sh
```
