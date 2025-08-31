# QFS Version 2.2.8 Release Notes

## Major Features and Improvements

### Java Compatibility Enhancements

- **Java 9+ Support**: Implemented new-style Java cleanup using `ref.Cleaner`
  while maintaining backward compatibility with pre-Java 9 versions
- **QFS Access Refactoring**: Split `KfsAccess` into `KfsAccess` and
  `KfsAccessBase` classes to support both reference cleaner interface (Java 9+)
  and finalize() methods (pre-Java 9)
- **Dual Module Support**: Created `qfs-access-pre-9` module for compatibility
  with older Java versions
- **Exception Handling**: Changed release and close methods to raise
  `IOException` for consistency

### Benchmark Improvements

- **MStress Hadoop 3.x Support**: Reworked MStress benchmark to use Hadoop 3.x
  APIs with `FileSystem` class instead of `DFSClient`
- **Self-contained JAR**: Build system now creates self-contained JAR with
  dependencies
- **Java 6 Compatibility**: Maintained compatibility by removing diamond
  operator usage

### Platform Support Updates

- **Amazon Linux 2023**: Added support for Amazon Linux 2023 builds
- **ARM64 Architecture**: Added ARM64 build support
- **Rocky Linux 9**: Improved Rocky Linux 9 build stability with cache
  management fixes
- **Ubuntu Updates**: Migrated CI builds from Ubuntu 18.04 to Ubuntu 20.04

## Component-Specific Changes

### QFS Client Library

- Fixed error handling with object store based chunk write lease maintenance
  logic
- Reset retry count after successful write ID allocation
- Removed unused includes in `Writer.cc`
- Fixed typos in method names

### Meta Server

- Fixed allocate RPC short RPC flag setting in re-allocate path with object
  store based chunks
- Added missing flag setting for proper operation

### Build System and CMake

- **Boost Compatibility**: Improved Boost version detection and compatibility
  - Fixed boost version conditionals for `BOOST_SP_USE_QUICK_ALLOCATOR`
  - Added `Boost_MINOR_VERSION` check in addition to `Boost_MAJOR_VERSION`
  - Used `Boost_MAJOR_VERSION` instead of `Boost_VERSION_MAJOR` for older
    FindBoost compatibility
  - Disabled deprecated allocator options for Boost 1.87+
- **CMake Improvements**:
  - Avoided `LESS_EQUAL` operator for older CMake versions
  - Reformatted and improved consistency of CMakeLists.txt files
  - Fixed comments and folded long lines
- **Maven Updates**: Updated Maven URLs to latest versions
- **YUM Package Management**: Improved package management for CentOS/Rocky Linux
  with cache clearing and `--nobest` option

### Common Library

- Added `constexpr` function qualifier definition for C++11+ compatibility
- Fixed template syntax errors for newer Clang versions
- Fixed typo in `StBuffer::Swap()` method
- Used `constexpr` for Base64 and CryptoKeys size calculations

### IO Library

- Enhanced Base64 size and padding calculation methods with `constexpr`
- Improved CryptoKeys size calculation methods

### GitHub Actions and CI

- Removed support for end-of-life distributions (Debian 10, Ubuntu 18.04)
- Implemented build optimizations to avoid rate limiting
- Limited parallel builds and connections to prevent repository fetch issues
- Re-enabled and stabilized Rocky Linux builds
- Added Amazon Linux and ARM64 build workflows

### Java Build System

- Removed extraneous project arguments from Maven commands
- Made variable names consistent in `javabuild.sh`
- Eliminated Java compiler warnings across multiple modules
- Fixed code formatting and style issues

### Testing and Scripts

- Reformatted endurance test scripts for consistency
- Fixed `ulimit -h` return value handling in test scripts
- Improved Docker invocation in build scripts
- Fixed syntax errors in various shell scripts

## Bug Fixes

### Notable Fixes

- **Object Store Chunks**: Fixed RPC flag setting for object store based chunk
  operations
- **Write Lease Maintenance**: Improved error handling and retry logic for chunk
  write operations
- **Template Compatibility**: Fixed template syntax issues with newer compiler
  versions

### Build and Compatibility Fixes

- Fixed CMake compatibility issues with older versions
- Resolved Boost library detection and usage issues
- Fixed Java finalizer handling for different Java versions
- Corrected Maven command arguments and build script issues

### Code Quality Improvements

- Eliminated compiler warnings across C++ and Java codebases
- Fixed code formatting and style issues
- Removed trailing whitespace and folded long lines
- Updated copyright years across affected files

## Platform and Dependency Updates

### Supported Platforms

- **Added**: Amazon Linux 2023, ARM64 architecture
- **Removed**: Debian 10 (end-of-life), Ubuntu 18.04 (CI only)
- **Improved**: Rocky Linux 9, CentOS 9 build stability

### Dependencies

- Updated Maven URLs to latest versions
- Improved Boost library compatibility (1.87+ support)
- Enhanced Java version support (Java 6 through Java 9+)

## Documentation and Maintenance

### Documentation Updates

- Updated Binary Distributions wiki page
- Added Amazon Linux 2023 to build status badges
- Updated README files with new platform support information

### Code Maintenance

- Updated copyright years across multiple files
- Improved code formatting and consistency
- Enhanced error messages and logging
- Cleaned up unused includes and dependencies

## Possible Breaking Changes

### Java API Changes

- Release and close methods in Java access classes now throw `IOException`
- Some internal class names have been changed (added "Base" suffix to native
  classes)
- New module structure with `qfs-access-pre-9` for older Java versions

### Build System Changes

- MStress benchmark now requires Hadoop 3.4.1 for full functionality
- Some CMake minimum version requirements may have changed due to syntax updates

## Migration Notes

### For Java Users

- Applications using Java 9+ will automatically use the new reference cleaner
  interface
- Pre-Java 9 applications will continue to use the traditional finalize()
  approach
- Exception handling may need updates due to new `IOException` throwing behavior

### For Build Systems

- Rocky Linux 9 and CentOS 9 builds now use `--nobest` option for yum updates
- Maven URL updates may require build script modifications
- Boost 1.87+ users will see deprecated allocator warnings resolved
