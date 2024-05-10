# A simple parser for my spark programs
import sys

def parse_arguments():
    # ARGUMENTS
    # 0 = program name
    # 1 = output file
    # 2 = input file

    input_dir = "UNUSED"
    output_dir = "output"
    if len(sys.argv) > 1:
        output_dir = sys.argv[1]

    return {'input': input_dir, 'output': output_dir}

def get_output_dir():
    return parse_arguments()['output']
