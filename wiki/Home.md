# Welcome to the QFS Wiki

Quantcast File System (QFS) is a high-performance, fault-tolerant, distributed
file system developed to support MapReduce processing, or other applications
reading and writing large files sequentially. This wiki provides various pieces
of information regarding QFS, its setup, best practices, etc.

If there are any specific questions or comments you have that aren't answered in
this wiki, please feel free to reach out to us through our mailing list
[**qfs-devel@googlegroups.com**][mailto] or search the archives at the
[QFS Developer Mailing List][group]. You can also ask questions, report bugs, or
request improvements to qfs on our [JIRA issue tracker][jira].

For an indepth view into QFS and how it works, please read the
[QFS paper presented at VLDB][paper].

Some selected pages for you to read are available below. A full list of all wiki
pages is available on the right sidebar.

- [[Introduction To QFS]]
- [[Administrator's Guide]]
- [[Deployment Guide]]
- [[Configuration Reference]]
- [[Developer Documentation]]
- [[Migration Guide]]
- [[Performance Comparison to HDFS]]
- [[QFS Client Reference]]
- [[External Resources]]
- [[QFS on S3]]

## Updating the QFS Wiki

The community is encouraged to update the QFS wiki as they wish. However, public
editing has been turned off on the github wiki itself. Instead, the wiki
documents are mirrored in the [`wiki`][wiki] directory of the QFS source code.
This allows for a few things:

- Wiki documentation is distributed along with the qfs source code
- The community can use pull requests to submit changes to the wiki
- The community can request updates to the wiki in the same pull request as code
  changes are made, keeping code and documentation in sync

If you would like to make a change to the wiki, please submit a pull request and
we will be happy to accept it!

![Quantcast](//pixel.quantserve.com/pixel/p-9fYuixa7g_Hm2.gif?labels=opensource.qfs.wiki)

[group]: http://groups.google.com/group/qfs-devel
[jira]: https://quantcast.atlassian.net
[mailto]: mailto:qfs-devel@googlegroups.com
[paper]: http://db.disi.unitn.eu/pages/VLDBProgram/pdf/industry/p808-ovsiannikov.pdf
[wiki]: https://github.com/quantcast/qfs/tree/master/wiki
