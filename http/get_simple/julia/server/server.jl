# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

using Arrow, HTTP, Random, Tables

function randint_nullable(n::Integer)
    v = Vector{Union{Missing, Int}}(undef, n)
    rand!(v, Int)
    return v
end

function get_stream()
    total_records = 100_000_000
    batch_len = 4096
    stream = Tables.partitioner(Iterators.partition(1:total_records, batch_len)) do indices
        nrows = length(indices)
        return (
            a = randint_nullable(nrows),
            b = randint_nullable(nrows),
            c = randint_nullable(nrows),
            d = randint_nullable(nrows)
        )
    end
    return stream
end

println("Serving on localhost:8008...")
HTTP.listen("127.0.0.1", 8008) do http::HTTP.Stream
    HTTP.setstatus(http, 200)
    HTTP.setheader(http, "Transfer-Encoding" => "chunked")
    batches = get_stream()
    HTTP.startwrite(http)
    buffer = IOBuffer()
    for batch in batches
        truncate(buffer, 0)
        Arrow.write(buffer, batch, file=false)
        nbytes = position(buffer)
        seekstart(buffer)
        write(http, "$(string(nbytes, base=16))\r\n")
        write(http, buffer)
        write(http, "\r\n")
    end
    write(http, "0\r\n\r\n")
end
