# OpenList + 139Yun concurrent upload — build branch

- Patch: branch `139-parallel-upload` (commits touching `drivers/139` on top of an upstream release).
- `.github/workflows/auto-build.yml` runs daily at 03:30 Asia/Shanghai (and on demand). When upstream
  publishes a new release or the patch changes, it cherry-picks the patch onto the release, runs the
  139 driver tests, builds `linux/amd64` and pushes `ghcr.io/eleost04/openlist:139-latest` and
  `:<release>-139-<patch sha>`.
- `latest.env` records the last successful build (upstream version, patch commit, image digest).
  Deploy by digest from this file.
- If the patch no longer applies, the run fails: rebase `139-parallel-upload` onto the new release by hand.
