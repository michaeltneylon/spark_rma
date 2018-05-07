#! /usr/bin/env Rscript
#
# Copyright 2018 Eli Lilly and Company
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# RMA -- Annotation and Background Correction -- processes single file for
# submitting to a GE cluster as an array job.

# reads the CEL file(s), annotates probes to summarization target level
# (transcript_cluster or probeset), and background corrects.

if (!require(docopt)) {
  install.packages("docopt", repos="http://cran.rstudio.com/")
}
if (!require(affyio)) {
  source("http://bioconductor.org/biocLite.R")
  biocLite("affyio")
}
if (!require(preprocessCore)) {
  source("http://bioconductor.org/biocLite.R")
  biocLite("preprocessCore")
}

library(preprocessCore, quiet=TRUE)
library(affyio, quiet=TRUE)
library(docopt, quiet=TRUE)

# ------------------------------------------------------------------------------
#  Command Line Interface
# ------------------------------------------------------------------------------

'usage: backgroundCorrect.R [-h] -i <input> -o <output> -p <probe_list> -a <annotation_type> [-f <output_format>]

RMA Annotation and Background Correction. Specify an input file, output path,
and the annotation file path. This only works on one input file at a time, to
support array jobs.

  -h --help             show this helpful message
  -i --input <input>    Absolute path to input CEL file (can be gzipped).
  -o --output <output>  Output path.
  -p --probe_list <probe_list>
                        R Binary file with list of data.frames containing
                        perfect match (PM) probes for the targets of interest
                        on the specific type of array.
  -a --annotation_type <annotation_type>  \
                        Choose the summarization target, either tc (transcript
                        cluster) or ps (probeset). choices={ps,tc}.
  -f --output_format <output_format>  \
                        Choose if you want a flat file or
                        compressed with gzip. choices={gzip,text}.\
                        [default: text]' -> doc
args <- docopt(doc)

# enforce choices. docopt doesn't support choices for named arguments
annotation_choices <- c('tc', 'ps')
if (!args$annotation_type %in% annotation_choices) {
  stop(paste('Invalid choice for annotation_type. Select from: ',
   paste(annotation_choices, collapse=", ")))
}
output_format_choices <- c('gzip', 'text')
if (!args$output_format %in% output_format_choices) {
  stop(paste('Invalid choice for output_format. Select from: ',
   paste(output_format_choices, collapse=", ")))
}

# ------------------------------------------------------------------------------
# helper functions
# ------------------------------------------------------------------------------

makeDirIfNotExist <- function(new_directory) {
  dir.create(new_directory, showWarnings = FALSE, recursive = TRUE, mode = '0775')
}

# create output parents path if it doesn't exist.
# strip off the basename, if we have a path..
if (grepl('/', args$output)) {
	  base <- basename(args$output)
	  parents <- dirname(args$output)
	  makeDirIfNotExist(parents)
}

# get summarization target (transcripts or probesets) and filter PM probes
load(args$probe_list)
if (args$annotation_type == 'tc') {
    transcript_annotation <- pm_list[['transcript_annotation']]
    pm_probes <- transcript_annotation$probe
    targets <- transcript_annotation$transcript_cluster
} else if (args$annotation_type == 'ps') {
    probeset_annotation <- pm_list[['probeset_annotation']]
    pm_probes <- probeset_annotation$probe
    targets <- probeset_annotation$probeset
}

# ------------------------------------------------------------------------------
# core functions
# ------------------------------------------------------------------------------

# RMA
# Background Correction step of RMA

# read CEL files and remove artifacts

backgroundCorrection <- function(input_file) {
  # function for background correction of CEL files.
  # Returns the output file name.
  message(paste("Background correcting", input_file, sep=': '))
  out_bg_file = args$output
  if (file.exists(out_bg_file)) {
    message(paste("File already exists!", out_bg_file))
  }
  else {
    celData <- read.celfile(input_file)
    load(args$probe_list)
    intensityMatrix <- matrix(celData$INTENSITY$MEAN, ncol=1)
    filteredMatrix <- intensityMatrix[pm_probes,, drop=FALSE]
    bg_corrected <- rma.background.correct(filteredMatrix)
    bg_corrected <- as.data.frame(bg_corrected)
    bg_corrected <- cbind(basename(input_file), pm_probes, targets,
                          bg_corrected)
    if (args$annotation_type == 'tc') {
        colnames(bg_corrected) <- c("sample", "probe", "transcript_cluster",
        "intensity_value")
    } else if (args$annotation_type == 'ps') {
        colnames(bg_corrected) <- c("sample", "probe", "probeset",
        "intensity_value")
    }
    if (args$output_format == 'gzip') {
      output_name = paste(out_bg_file, 'gz', sep='.')
      message(paste("writing to: ", output_name, sep=""))
      gzipped = gzfile(output_name, 'w')
      write.table(bg_corrected, gzipped, col.name=TRUE, row.names=FALSE,
                  sep='\t', quote=FALSE)
      close(gzipped)
    } else {
    message(paste("writing to: ", out_bg_file, sep=""))
    write.table(bg_corrected, file=out_bg_file, col.name=TRUE, row.names=FALSE,
                sep='\t', quote=FALSE)
    }
  }
}


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

backgroundCorrection(args$input)

message("Complete.")
