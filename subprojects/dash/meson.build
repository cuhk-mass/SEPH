project('dash','cpp',   
    version : '0.0.1',
    default_options : ['buildtype=debugoptimized', 'warning_level=3', 'cpp_std=gnu++20', 'c_std=gnu17'])

inc = include_directories('include')
deps = []
deps += dependency('fmt')
deps += dependency('threads')
deps += dependency('libpmem')
deps += dependency('libpmemobj')
deps += dependency('libpmemobj++')
deps += dependency('libpmempool')

args = ['-mavx512f']
# executable('dummy', 'src/dummy.cpp', include_directories:inc, dependencies:deps)
dash = static_library('dash', ['src/dash.cpp'], include_directories:inc, cpp_args:args, dependencies:deps)


dash_dep = declare_dependency(include_directories:inc, link_with:dash)