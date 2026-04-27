# pscp
parallel scp/rsync using multiple streams to copy remote data

# usage
```
usage: pscp2.py -s SOURCE -d DEST [options]

Copy files/directories with multiple parallel rsync streams.

options:
  -h, --help            show this help message and exit
  -s SOURCE, --source SOURCE
                        source path or user@host:path
  -d DEST, --dest DEST  destination path or user@host:path
  -a ARGS, --args ARGS  additional rsync arguments, shell-style quoted; default: '-a'
  -t THREADS, --threads THREADS
                        number of parallel rsync processes; default: 4
  -w DELAY, --delay DELAY
                        maximum random startup delay per transfer in milliseconds; default: 3.0
  -v, --verbose         print every rsync command
  -x, --dryrun          print commands without transferring
```

# requirements
The softweare depends only on the Python standard library (e.g., `argparse`, `subprocess`, `multiprocessing`, `os`, `sys`, `time`, `random`, `pathlib`). So there are no third-party Python packages required.
Both systems however need `rsync` and `openssh-client`, installed. The respective "server" also needs `openssh-server` installed. (This is the same as in the non-parallel scp or remote rsync case)
