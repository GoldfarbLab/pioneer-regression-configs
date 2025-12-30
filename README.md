# pioneer-regression-configs

## Dataset tags

The `params/dataset_tags.json` file maps each dataset directory name under `params/` to a set of tags.

Schema:

```json
{
  "datasets": {
    "<dataset_dir>": { "tags": ["fast"] }
  }
}
```

To mark a dataset as fast, add the string `"fast"` to its `tags` array.
