#!/bin/bash
set -e
checks_failed=false

# Git
echo "Changed files:"
git diff --name-status $GIT_REF..
readarray -t CHANGED_FILES <<< $(git diff --name-only $GIT_REF..)
CHANGED_FILES=${CHANGED_FILES[@]}

echo "Checking commit formatting..."
touch git_checks.md
bash tools/ci/git.sh $GIT_REF >> git_checks.md || true
if [[ -s git_checks.md ]]; then
    checks_failed=true
fi

echo "Checking license headers..."
python3 tools/ci/detect_license_headers.py >> license_headers_checks.txt

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
if [[ -s python_checks.txt ]]; then
    checks_failed=true
fi

# SQL
echo "Running SQL checks..."
touch SQL_checks.txt
for changed_file in $CHANGED_FILES; do
    if [[ -f $changed_file ]]; then
        if [[ $changed_file == *.sql ]]; then
            bash tools/ci/sql.sh ${changed_file} >> SQL_checks.txt || true
        fi
    fi
done
python3 tools/price_checker.py >> SQL_checks.txt
if [[ -s SQL_checks.txt ]]; then
    checks_failed=true
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
if [[ -s lua_checks.txt ]]; then
    checks_failed=true
fi

# C++
echo "Running C++ checks..."
touch c++_checks.txt
for changed_file in $CHANGED_FILES; do
    if [[ -f $changed_file ]]; then
        if [[ $changed_file == *.cpp ]]; then
            bash tools/ci/cpp.sh ${changed_file} 2>> c++_checks.txt || true
        fi
    fi
done
if [[ -s c++_checks.txt ]]; then
    checks_failed=true
fi

echo "Running C++ formatting checks (clang-format-18)..."
clang-format-18 -version
touch c++_formatting_checks.md
for changed_file in $CHANGED_FILES; do
    if [[ -f $changed_file ]]; then
        if [[ $changed_file == *.cpp || $changed_file == *.h ]]; then
            clang-format-18 -style=file -i ${changed_file}
        fi
    fi
done

git diff --no-color >> c++_formatting_checks.md
if [[ -s c++_formatting_checks.md ]]; then
    checks_failed=true
    sed -i '1i \`\`\`diff' c++_formatting_checks.md
    echo "\`\`\`" >> c++_formatting_checks.md
    echo "" >> c++_formatting_checks.md
fi

# git diff --color >> cpp_formatting_checks_color.txt
# if [[ -s cpp_formatting_checks_color.txt ]]; then
#     echo ""
#     echo "You have errors in your C++ code formatting."
#     echo "Please see below in red for the incorrect formatting, and in green for the correct formatting."
#     echo "You can either fix the formatting by hand or use clang-format."
#     echo "(You can safely ignore warnings about \$TERM and tput)"
#     echo ""
#     cat cpp_formatting_checks_color.txt | diff-so-fancy || true
#     checks_failed=true
# fi

if [[ "$checks_failed" == "true" ]]; then
    echo "One or more checks failed."
    exit 1
else
    echo "All checks passed."
    exit 0
fi
