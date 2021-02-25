# congregation plaintext demo

This demo illustrates a simple sum aggregation between 3 parties over secret shared data, and is intended to be run \
locally. The jiff server is run by party 1, but you can modify the `server_ip`, `server_port`, and `server_pid` \
entries in the config files at `party_one/config.json` / `party_two/config.json` / `party_three/config.json` \
if you would rather run it separately.

For any other kind of customization, just refer to the [Wiki](https://github.com/CCD-HRI/congregation/wiki).

## input data

The input files located at `party_one/data/inpt.csv`, `party_two/data/inpt.csv`, and `party_three/data/inpt.csv` \
represent serialized secret shares using the [liturgy](https://github.com/multiparty/liturgy) library. The dataset 
yielded by combining these shares looks like this:

```csv
a,b,c
1,2,3
4,5,6
```

- Note there is an extra column called `keepRows` in the share files. Internally, congregation uses a secret shared \
column of that name to keep track of which rows are valid / invalid through various MPC operations. The `liturgy` \
library automatically appends this column when sharing an input dataset, so that it can be ingested by congregation's \
code generation module.  
  
## run

Once you've [installed](https://github.com/CCD-HRI/congregation/wiki/Installation) the library, run `demo.py`
from this directory in three separate terminal windows as follows:

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

The output will be written to each party's `data` directory. If you haven't edited the protocol at all, it will \
be in a file called `agg_collect.csv`, and should look like:

```csv
a
5
```

- Note that you'll have to manually kill the process running the jiff server after each computation. If a compute \
  party is running the server, it's PID will be displayed once it's launched during the computation.