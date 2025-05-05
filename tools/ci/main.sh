#!/bin/bash
set -e

# Git
echo "Checking commit formatting..."
touch git_checks.txt
bash tools/ci/git.sh >> git_checks.txt || true
cat git_checks.txt
if [[ -s git_checks.txt ]]; then
    exit 1
fi

echo "Changed files:"
git diff --name-status HEAD^{/"Merge pull request"}...HEAD
readarray -t CHANGED_FILES <<< $(git diff --name-only HEAD^{/"Merge pull request"}...HEAD)
CHANGED_FILES=${CHANGED_FILES[@]}

echo "Checking license headers..."
python3 tools/ci/detect_license_headers.py

# Python
echo "Running Python checks..."
touch python_checks.txt
for changed_file in $CHANGED_FILES; do
    if [[ -f $changed_file ]]; then
        if [[ $changed_file == *.py ]]; then
            bash tools/ci/python.sh ${changed_file} >> python_checks.txt || true
        fi
    fi
done
cat python_checks.txt
if [[ -s python_checks.txt ]]; then
  exit 1
fi

# SQL
echo "Running SQL checks..."
touch sql_checks.txt
for changed_file in $CHANGED_FILES; do
    if [[ -f $changed_file ]]; then
        if [[ $changed_file == *.sql ]]; then
            bash tools/ci/sql.sh ${changed_file} >> sql_checks.txt || true
        fi
    fi
done
python3 tools/price_checker.py >> sql_checks.txt
cat sql_checks.txt
if [[ -s sql_checks.txt ]]; then
    exit 1
fi

# Lua
echo "Running Lua checks..."
touch lua_checks.txt
python3 tools/ci/lua_stylecheck.py test >> lua_checks.txt
for changed_file in $CHANGED_FILES; do
    if [[ -f $changed_file ]]; then
        if [[ $changed_file == *.lua ]]; then
            bash tools/ci/lua.sh ${changed_file} >> lua_checks.txt || true
        fi
    fi
done
python3 tools/ci/check_lua_binding_usage.py >> lua_checks.txt
cat lua_checks.txt
if [[ -s lua_checks.txt ]]; then
    exit 1
fi

# C++
echo "Running C++ checks..."
touch cpp_checks.txt
for changed_file in $CHANGED_FILES; do
    if [[ -f $changed_file ]]; then
        if [[ $changed_file == *.cpp ]]; then
            bash tools/ci/cpp.sh ${changed_file} 2>> cpp_checks.txt || true
        fi
    fi
done
cat cpp_checks.txt
if [[ -s cpp_checks.txt ]]; then
    exit 1
fi

echo "Running C++ formatting checks (clang-format-18)..."
clang-format-18 -version
touch cpp_formatting_checks.txt
for changed_file in $CHANGED_FILES; do
    if [[ -f $changed_file ]]; then
        if [[ $changed_file == *.cpp || $changed_file == *.h ]]; then
            clang-format-18 -style=file -i ${changed_file}
        fi
    fi
done
git diff --color >> cpp_formatting_checks_color.txt
if [[ -s cpp_formatting_checks_color.txt ]]; then
    echo ""
    echo "You have errors in your C++ code formatting."
    echo "Please see below in red for the incorrect formatting, and in green for the correct formatting."
    echo "You can either fix the formatting by hand or use clang-format."
    echo "(You can safely ignore warnings about \$TERM and tput)"
    echo ""
    cat cpp_formatting_checks_color.txt | diff-so-fancy || true
    exit 1
fi

echo "Running Lua Language Server..."
python3 tools/ci/lua_lang_server.py
if [[ -f lua_lang_errors.txt ]]; then
    cat lua_lang_errors.txt
    exit 1
fi

exit 0
