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

using Arrow, HTTP, BufferedStreams

function process_batches()
    buffer = Vector{UInt8}()
    record_batches = []
    HTTP.open("GET", "http://localhost:8008") do http_io
        startread(http_io)
        buffered_io = BufferedInputStream(http_io)
        while !eof(buffered_io)
            # Get length of chunk
            len = parse(Int, readline(buffered_io), base=16)
            # We are done once we hit a zero-length chunk
            len == 0 && break
            resize!(buffer, len)
            n_read = 0
            while n_read < len
                n_read += readbytes!(buffered_io, @view(buffer[n_read+1:end]), len - n_read)
            end
            # We are getting streams with one record batch in them
            # So before we stack them in record_batches we grab the 
            # one and only record batch in the stream we just downloaded. 
            push!(record_batches, collect(Arrow.Stream(buffer))[1])
            readline(buffered_io) # empty line
        end
    end
    record_batches
end

# warmup run
batches = process_batches()
# benchmark run
execution_time = @elapsed batches = process_batches()

println("$(execution_time) seconds elapsed")
println("$(length(batches)) record batches received")