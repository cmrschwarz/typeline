macro:=mv
    c="[
        [ 'key', '_in'],
        [ 'key', '_out', [ 'f', _[1] ] ],
        [ 'exec', 'mv', '{_in}', '{_out}']
    ]"
end

macro:=ls
    c="[
        [ 'exec', 'ls', '-1'],
        [ 'lines']
    ]"
end