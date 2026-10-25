# Introduction

dynamo-gdl is a graph definition language and runtime for distributed request/response streaming pipelines built on [velo](https://github.com/ai-dynamo/velo). A graph is defined once with its message types. A placement config splits it into segments. A generator emits one crate per segment, and each crate links only the node implementations placed in it.
