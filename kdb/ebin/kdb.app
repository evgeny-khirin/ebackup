{application, kdb,
 [{description, "Erlios KDB"},
  {vsn, "1.0.1"},
  {modules, [kdb,
	     kdb_drv]},
  {registered,[]},
  {applications, [kernel, stdlib, eklib]},
  {env, []}]}.
