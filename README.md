# ETH Zurich - Web Scale Data Mining Project

This is the main repository for the web scale data mining project, which took place in summer 2014 as a research project.

## Directory Structure and Overview

<pre>
├── doc - the report of the project
└── src - the source code
    ├── WSDA
    ├── combine_sequence_files
    ├── examples
    ├── html_to_text_conversion
    ├── remove_infrequent_words
    ├── results_display
    ├── scripts
    └── word_count
</pre>

### Source code projects

#### WSDA

The self-implemented LDA

TODO: add some more doc, references, etc.

@hany-abdelrahman: the WSDA directory should probably be renamed to something more meaningful.

Author: [Hany Abdelrahman](https://github.com/hany-abdelrahman)

#### combine_sequence_files

Combines sequence files (http://wiki.apache.org/hadoop/SequenceFile) from subdirectories
into multiple sequence files. These sequence files have the same name as the subdirectories.

This way, it is possible to create a flat directory structure whith few large sequence files.

Author: [Lukas Elmer](https://github.com/lukaselmer)



