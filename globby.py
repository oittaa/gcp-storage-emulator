from wcmatch import glob

def glob_with_array(file_paths, pattern):
    matched_files = [
        path for path in file_paths if
        glob.globmatch(path, pattern, flags=glob.GLOBSTAR | glob.BRACE) 
        or
        glob.globmatch(path, pattern.replace('**', '*/*'), flags=glob.GLOBSTAR | glob.BRACE | glob.EXTGLOB)
    ]
    return matched_files

def test_match_glob():
    # File paths with distinct patterns

    matcher = glob.globmatch("foo/bar", "foo*bar", flags=glob.GLOBSTAR | glob.BRACE | glob.EXTGLOB)
    print(f"{matcher}")

    blob_names = ["foo/bar", "foo/baz", "foo/foobar", "foobar"]

    match_glob_results = {
        "foo*bar": ["foobar"],
        "foo**bar": ["foo/bar", "foo/foobar", "foobar"],
        "**/foobar": ["foo/foobar", "foobar"],
        "*/ba[rz]": ["foo/bar", "foo/baz"],
        "*/ba[!a-y]": ["foo/baz"],
        "**/{foobar,baz}": ["foo/baz", "foo/foobar", "foobar"],
        "foo/{foo*,*baz}": ["foo/baz", "foo/foobar"],
    }

    # Iterate through the match glob patterns and expected results
    for match_glob, expected_names in match_glob_results.items():
        glob_results = glob_with_array(blob_names, match_glob)
        print(f"\n")
        if glob_results == expected_names:
            print(f"Matched: {match_glob}")
        else:
            print(f"Not matched: {match_glob}")
            print(f"Expected: {expected_names}")
            print(f"Actual: {glob_results}")
    print(f"\n")

test_match_glob()