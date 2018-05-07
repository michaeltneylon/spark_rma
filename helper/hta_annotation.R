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

# Generate HTA 2.0 perfect match probe list for core genes/transcripts and
# probesets to filter CEL file list before background correction

# install/load the bioconductor HTA 2.0 annotation database
message('Loading pd.hta.2.0 from bioconductor to export files for RMA')

if (!require(pd.hta.2.0)) {
  source("https://bioconductor.org/biocLite.R")
  biocLite("pd.hta.2.0", dependencies=TRUE)
}
library(pd.hta.2.0)
conn <-db(pd.hta.2.0)

# get probe annotation mapping to transcript clusters and probesets.
message('Generating annotation file...')
fullAnnotation <- dbGetQuery(conn,
  'SELECT ps.fid as probe, probeset, transcript_cluster from
  (SELECT fid, fsetid, man_fsetid as probeset from pmfeature INNER JOIN
   featureSet USING(fsetid)) ps
  LEFT JOIN
  (SELECT fid, fsetid, transcript_cluster_id as transcript_cluster from
   pmfeature INNER JOIN core_mps USING(fsetid)) tc
  ON ps.fsetid = tc.fsetid and ps.fid = tc.fid
  UNION
  SELECT ps.fid as probe, probeset, transcript_cluster from
  (SELECT fid, fsetid, transcript_cluster_id as transcript_cluster from
   pmfeature INNER JOIN core_mps USING(fsetid)) tc
  LEFT JOIN
  (SELECT fid, fsetid, man_fsetid as probeset from pmfeature INNER JOIN
   featureSet USING(fsetid)) ps
  ON tc.fsetid = ps.fsetid and tc.fid = ps.fid')

probeset_level <- unique(fullAnnotation[,c('probe', 'probeset')])
# remove NA
probeset_annotation <- probeset_level[complete.cases(probeset_level),]

# this is not taken as unique, here we see how some probes can map to multiple probesets that map to the same transcript
transcript_level <- fullAnnotation[,c('probe', 'transcript_cluster')]
# remove NA
transcript_annotation <- transcript_level[complete.cases(transcript_level),]

pm_list <- list(probeset_annotation, transcript_annotation)
names(pm_list) <- c('probeset_annotation', 'transcript_annotation')

# save as R data file for faster loading in R background correction step.
save(pm_list, file='pm_probe_annotation.rda')
message('Done. PM probes saved to R data file pm_probe_annotation.rda, as a
list named pm_list with two data.frames, named probeset_annotation and transcript_annotation, representing the PM probes
 and probesets and transcripts respectively. Each has two columns, probe and probeset or transcript_cluster.')
