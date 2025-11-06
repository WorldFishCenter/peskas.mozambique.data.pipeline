# Rename Nested Survey Data Elements

Appends a parent name or index to child elements within a nested list,
assisting in creating a coherent and traceable data structure during the
flattening process.

## Usage

``` r
rename_child(x, i, p)
```

## Arguments

- x:

  A list element, possibly nested, to be renamed.

- i:

  The index or key of the element within the parent list.

- p:

  The parent name to prepend to the element's existing name for context.

## Value

A renamed list element, structured to maintain contextual relevance in a
flattened dataset.
