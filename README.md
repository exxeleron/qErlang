qErlang 
=======

qErlang is an Erlang module implementing kdb+ interprocess communication protocol. 
qErlang provides:
- Support for synchronous and asynchronous queries
- Support for Kdb+ protocol type 3.0
- Full coverage of KDB+ types
- Partial decoding of messages for fast query routing
- Compatibility with Erlang/OTP 17.x

## Usage

To use qErlang from rebar, add:

```erlang
{deps, [{qErlang, ".*", {git, "https://github.com/exxeleron/qErlang"}}]}.
```

to your rebar.config.

## Documentation

EDoc generated documentation of the API is available [here](http://exxeleron.github.io/qErlang/qErlang.html).

## Examples

The [`examples`](https://github.com/exxeleron/qErlang-dev/tree/master/examples/tick_subscribe) directory contains an example OTP application with a gen_server subscribing to a KDB+ tick instance for updates.

In qErlang, q types deserialize to Erlang tuples of the form `{<q type>, <data>}`. The [`qErlang_test.erl`](https://github.com/exxeleron/qErlang-dev/blob/master/test/qErlang_test.erl) file in the `test` directory is a useful reference with examples of deserialized q objects of various types.
