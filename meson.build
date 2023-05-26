project('pmhb','cpp',   
    version : '0.0.1',
    default_options : ['buildtype=release', 'warning_level=3', 'cpp_std=gnu++20', 'c_std=gnu17'])


# macros = ['-DPMHB_LATENCY', '-DLOAD_FACTOR', '-DCOUNTING_WRITE', '-DINSERT_DEBUG', '-DPREFAULT']

if get_option('PMHB_LATENCY') == true
    add_global_arguments('-DPMHB_LATENCY', language:'cpp')
endif

if get_option('LOAD_FACTOR') == true
    add_global_arguments('-DLOAD_FACTOR', language:'cpp')
endif

if get_option('COUNTING_WRITE') == true
    add_global_arguments('-DCOUNTING_WRITE', language:'cpp')
endif

if get_option('INSERT_DEBUG') == true
    add_global_arguments('-DINSERT_DEBUG', language:'cpp')
endif

if get_option('PREFAULT') == true
    add_global_arguments('-DPREFAULT', language:'cpp')
endif

if get_option('BREAKDOWN_SOD') == true
    add_global_arguments('-DBREAKDOWN_SOD', language:'cpp')
endif

if get_option('BREAKDOWN_SO') == true
    add_global_arguments('-DBREAKDOWN_SO', language:'cpp')
endif

if get_option('BREAKDOWN_S') == true
    add_global_arguments('-DBREAKDOWN_S', language:'cpp')
endif

if get_option('BREAKDOWN_BASE') == true
    add_global_arguments('-DBREAKDOWN_BASE', language:'cpp')
endif

# dummy_proj = subproject('dummy')
# dummy_dep = dummy_proj.get_variable('dummy_dep')

steph_proj = subproject('steph')
steph_dep = steph_proj.get_variable('steph_dep')

level_proj = subproject('level')
level_dep = level_proj.get_variable('level_dep')

cceh_proj = subproject('cceh')
cceh_dep = cceh_proj.get_variable('cceh_dep')

cceh_cow_proj = subproject('cceh_cow')
cceh_cow_dep = cceh_cow_proj.get_variable('cceh_cow_dep')

dash_proj = subproject('dash')
dash_dep = dash_proj.get_variable('dash_dep')

clevel_proj = subproject('clevel')
clevel_dep = clevel_proj.get_variable('clevel_dep')

pclht_proj = subproject('pclht')
pclht_dep = pclht_proj.get_variable('pclht_dep')


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
# deps += dummy_dep
deps += steph_dep
deps += level_dep
deps += cceh_dep
deps += cceh_cow_dep
deps += dash_dep
deps += clevel_dep
deps += pclht_dep

args = ['-mavx512f']

executable('pmhb', src, include_directories:inc, cpp_args:args, dependencies:deps)