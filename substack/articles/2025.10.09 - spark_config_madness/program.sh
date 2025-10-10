echo "Running all the Spark scripts in sequence..."

# Function to run script with filtered output using external filter file
run_script() {
    local script_name=$1
    echo "Running $script_name..."
    echo "-----------------------------------"
    echo ""
    
    # Build grep command from filter file
    local grep_cmd="cat"
    while IFS= read -r pattern; do
        # Skip empty lines and comments
        [[ -z "$pattern" || "$pattern" =~ ^#.* ]] && continue
        grep_cmd="$grep_cmd | grep -v \"$pattern\""
    done < spark_ignore_msgs.txt
    
    uv run "$script_name" 2>&1 | eval "$grep_cmd"
}

run_script "spark_iceberg_glue.py"
#run_script "spark_iceberg_rest.py" 
#run_script "spark_s3.py"
#run_script "spark_combined.py"