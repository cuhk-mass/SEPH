project('dummy','cpp',   
    version : '0.0.1',
    default_options : ['buildtype=debugoptimized', 'warning_level=3', 'cpp_std=gnu++20', 'c_std=gnu17'])



# ================ config.h ===============================
# conf_data = configuration_data()
# t_arg = get_option('something')
# if t_arg == true
#     conf_data.set('something1', '1234')
# else
#     conf_data.set('something1', '123')
# endif 
# configure_file(input:'config.h.in', output:'config.h', configuration:conf_data)
# ================ config.h ===============================

inc = include_directories('include')
deps = []
deps += dependency('fmt')

args = ['-fPIC']
# executable('dummy', 'src/dummy.cpp', include_directories:inc, dependencies:deps)
dummy = static_library('dummy', 'src/dummy.cpp', include_directories:inc, cpp_args:args, dependencies:deps)


dummy_dep = declare_dependency(include_directories:inc, link_with:dummy)