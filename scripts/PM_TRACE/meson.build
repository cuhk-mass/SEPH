project('trace_maker','cpp',   
    version : '0.0.1',
    default_options : ['buildtype=release', 'warning_level=3', 'cpp_std=gnu++20', 'c_std=gnu17'])



inc = []
inc += include_directories('include')


src = ['src/main.cpp']

deps = []
deps += dependency('fmt')
deps += dependency('threads')
deps += dependency('libpmem')
deps += dependency('libpmemobj')
deps += dependency('libpmemobj++')
deps += dependency('libpmempool')


args = ['-mavx512f']

executable('trace_maker', src, include_directories:inc, cpp_args:args, dependencies:deps)