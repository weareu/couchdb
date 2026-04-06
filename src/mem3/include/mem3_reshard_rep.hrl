% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License.

-record(split_state, {
    source :: #shard{},
    targets :: [#shard{}],
    target_map :: #{},
    factor :: pos_integer(),
    state :: atom(),
    error :: term() | undefined
}).
