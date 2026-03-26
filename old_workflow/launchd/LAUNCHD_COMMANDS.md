# Launchd Commands

## Install

```bash
mkdir -p ~/Library/LaunchAgents
cp /Users/bcnm@mediait.ch/Projects/work/iran/data/iran_krieg_daily/ch.srf.iran-krieg.reduce-points.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/ch.srf.iran-krieg.reduce-points.plist
```

## Run Once

```bash
launchctl kickstart -k gui/$(id -u)/ch.srf.iran-krieg.reduce-points
```

## Status

```bash
launchctl print gui/$(id -u)/ch.srf.iran-krieg.reduce-points
cat /Users/bcnm@mediait.ch/Projects/work/iran/data/iran_krieg_daily/logs/launchd_stdout.log
cat /Users/bcnm@mediait.ch/Projects/work/iran/data/iran_krieg_daily/logs/launchd_stderr.log
```

## Remove

```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/ch.srf.iran-krieg.reduce-points.plist
rm -f ~/Library/LaunchAgents/ch.srf.iran-krieg.reduce-points.plist
```
