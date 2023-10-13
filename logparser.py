from logparser.Drain import LogParser

input_dir = '.' # The input directory of log file
output_dir = 'result/'  # The output directory of parsing results
log_file = 'sample.log'  # The input log file name
log_format = '<Level> <Content>' # Define log format to split message fields

parser = LogParser(log_format, indir=input_dir, outdir=output_dir )
parser.parse(log_file)
