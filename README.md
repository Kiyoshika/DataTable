# DataTable
This is a work in progress library to build a data table/data set library similar to "Pandas" from Python but for C with a much better API.

Eventually I want to wrap all of this code in a Python and perhaps Julia wrapper and make it available through pip or other package managers.

## Background 
I work as a data scientist and use Pandas pretty often. Unfortunately, using Pandas is some of the most pain I have to deal with. Personally, I feel like the Pandas API is incredibly unintuitive. For example, inserting a row into a data frame involves convoluted syntax. I wanted to build similar functionality but with a much more readable syntax and, who knows, maybe even be a bit faster.

## Components
### DataTable
This is a data structure composed of `DataColumn`s (see below). All the columns are allocated contiguously and are tied with a `char*` for its column name. We can select, filter, create, manipulate, etc. columns within the table.
### DataColumn
This is a data structure that represents a single column. All of its members are allocated contiguously and supports all the typical types (`int32_t`, `uint8_t`, `char*`, etc.) and also custom user types (so we can have tables of more complex structures if required).

## Documentation
Want to write some documentation once I have some of the core functionality built.

## Building from Source
This is a typical CMake project. You can include it directly in your build or build it separately. For CMake users, you can use `target_include_directories([target] PUBLIC ${DataTable_SOURCE_DIR}/include)` and `target_link_libraries([target] datatable)` after using `add_subdirectory(DataTable)` to add it to your project's build.
