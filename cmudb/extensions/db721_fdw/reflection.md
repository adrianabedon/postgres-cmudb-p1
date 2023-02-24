# Reflection
## Strengths
- Support for multiple types.
- Block skipping can significantly improve performance depending on the query.
- Having metadata in a json format makes parsing very easy since existing libraries can be used. 
- Organizing columns into block allows for an simple amount of data to be read from disk into memory.

## Weaknesses
- Blocks are not fixed size. Block size varies depending on the type in the block. While this made the programming easier since all blocks had the same number of values, it means performance differs between blocks since different amount of data have to be read from disk depending on the type. 
- Since min and max values for blocks share the type of the block, they must be a supported json type which limits which types can be used. Supporting a new type could be done by econding the type in a string and then parsing which adds another layer of complexity.
- Strings always have the exact same size of 32 bytes. This means that shorter strings waste lots of space and that strings longer than 32 bytes are not supported. 
## Suggestions
- Currently, column 1's block x and column 2's block x will always have the same number of values. Since this is always true in the file format, it would be good to make it explicit and remove the number of values from the column's block_stats and have a separate table with block index -> num of values at the root of the metadata.
- Encode strings as [str_length | str_data] similar to how postgres encodes their strings. This would allow for support for variable length strings. 