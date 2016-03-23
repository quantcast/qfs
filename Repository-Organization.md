```
    QFS top-level directory
    │
    ├──── benchmarks
    │     └──── mstress                (Benchmark comparing meta operations)
    │
    ├──── build                        (Build output directory)
    │
    ├──── examples
    │     ├──── cc                     (C++ client example)
    │     ├──── java                   (Java client example)
    │     ├──── python                 (Experimental python client example)
    │     └──── sampleservers          (Scripts to bring up local QFS for testing)
    │
    ├──── contrib                      (Contributions including sample deployment packages)
    │
    ├──── conf                         (Sample configs for release packaging)
    │
    ├──── scripts                      (Admin and init scripts)
    │
    ├──── webui                        (metaserver web UI monitor)
    │
    └──── src
          │
          ├──── cc
          │     ├──── access           (Java/Python glue code)
          │     ├──── chunk            (chunk server code)
          │     ├──── common           (common declarations)
          │     ├──── devtools         (miscellaneous developer utilities & tools)
          │     ├──── emulator         (QFS offline layout emulator) 
          │     ├──── fuse             (FUSE module for Linux and MacOS)
          │     ├──── kfsio            (I/O library used by QFS)
          │     ├──── libclient        (client library code)
          │     ├──── meta             (metaserver code)
          │     ├──── qcdio            (low level io and threading library)
          │     ├──── qcrs             (Reed-Solomon encoder and decoder)
          │     ├──── tests            (QFS tests)
          │     └──── tools            (QFS user/admin tools)
          │
          ├──── java
          │     ├──── qfs-access       (Java wrappers to call QFS-JNI code)
          │     └──── hadoop-qfs       (Apache Hadoop QFS plugin)
          │
          ├──── python                 (Experimental python client code)
          └──── test-scripts           (Scripts to test QFS servers and components)
```

![Quantcast](//pixel.quantserve.com/pixel/p-9fYuixa7g_Hm2.gif?labels=opensource.qfs.wiki)