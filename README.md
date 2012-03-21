HashSplitter analysis plugin for ElasticSearch
==============================================

HashSplitter plugin is a N-Gram tokenizer generating tokens that are not overlapping and are prefixed.

It is aimed at making hashs (or any fixed length value splittable in equally sized chunks) partially searchables, without using a wildcard query.
It can also help reduce the index size.
However, depending on your configuration, if you do not wish to search for wildcard queries, you may experience slightly decreased performance.
See http://elasticsearch-users.115913.n3.nabble.com/Advices-indexing-MD5-or-same-kind-of-data-td2867646.html for more information.


In order to install the plugin, simply run: `bin/plugin -install yakaz/elasticsearch-analysis-hashsplitter/master`.

    -------------------------------------------------
    | HashSplitter Analysis Plugin | ElasticSearch  |
    -------------------------------------------------
    | master                       | 0.19 -> master |
    -------------------------------------------------

The plugin includes the `hash_splitter` tokenizer and token filter.
