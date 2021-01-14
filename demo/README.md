# congregation demo

This demo illustrates a simple sum aggregation between 
3 parties, and is intended to be run locally. The jiff
server is run by party 1, but you can modify the `server_ip`,
`server_port`, and `server_pid` entries in the config files
at `party_one/config.json` / `party_two/config.json` / `party_three/config.json`
if you would rather run it separately.

For any other kind of customization, refer to the [Wiki](https://github.com/CCD-HRI/congregation/wiki)

## run

Once you've [installed](https://github.com/CCD-HRI/congregation/wiki/Installation) 
the library, run `demo.py` from this directory in three separate 
terminal windows as follows:

#### party one
```shell
> python demo.py party_one/config.json </path/to/jiff/lib/>
```
#### party two
```shell
> python demo.py party_two/config.json </path/to/jiff/lib/>
```
#### party three
```shell
> python demo.py party_three/config.json </path/to/jiff/lib/>
```

The output will be written to each party's `data` directory. If
you haven't edited the protocol at all, it will be in a file called
`agg_collect.csv`.