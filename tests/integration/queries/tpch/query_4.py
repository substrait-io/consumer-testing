TESTCASE = [
    {
        "test_name": "test_tpch_sql_4",
        "file_names": ["orders_1.parquet", "lineitem_1.parquet"],
        "sql_query":
            """
            SELECT
                o_orderpriority,
                count(*) AS order_count
            FROM
                '{}'
            WHERE
                o_orderdate >= CAST('1993-07-01' AS date)
                AND o_orderdate < CAST('1993-10-01' AS date)
                AND EXISTS (
                    SELECT
                        *
                    FROM
                        '{}'
                    WHERE
                        l_orderkey = o_orderkey
                        AND l_commitdate < l_receiptdate)
            GROUP BY
                o_orderpriority
            ORDER BY
                o_orderpriority;
            """,
        "substrait_query":
            """
            {
              "extensionUris": [{
                "extensionUriAnchor": 4,
                "uri": "/functions_aggregate_generic.yaml"
              }, {
                "extensionUriAnchor": 1,
                "uri": "/functions_boolean.yaml"
              }, {
                "extensionUriAnchor": 2,
                "uri": "/functions_datetime.yaml"
              }, {
                "extensionUriAnchor": 3,
                "uri": "/functions_comparison.yaml"
              }],
              "extensions": [{
                "extensionFunction": {
                  "extensionUriReference": 1,
                  "functionAnchor": 0,
                  "name": "and:bool"
                }
              }, {
                "extensionFunction": {
                  "extensionUriReference": 2,
                  "functionAnchor": 1,
                  "name": "gte:date_date"
                }
              }, {
                "extensionFunction": {
                  "extensionUriReference": 2,
                  "functionAnchor": 2,
                  "name": "lt:date_date"
                }
              }, {
                "extensionFunction": {
                  "extensionUriReference": 3,
                  "functionAnchor": 3,
                  "name": "equal:any1_any1"
                }
              }, {
                "extensionFunction": {
                  "extensionUriReference": 4,
                  "functionAnchor": 4,
                  "name": "count:opt"
                }
              }],
              "relations": [{
                "root": {
                  "input": {
                    "sort": {
                      "common": {
                        "direct": {
                        }
                      },
                      "input": {
                        "aggregate": {
                          "common": {
                            "direct": {
                            }
                          },
                          "input": {
                            "project": {
                              "common": {
                                "emit": {
                                  "outputMapping": [9]
                                }
                              },
                              "input": {
                                "filter": {
                                  "common": {
                                    "direct": {
                                    }
                                  },
                                  "input": {
                                    "read": {
                                      "common": {
                                        "direct": {
                                        }
                                      },
                                      "baseSchema": {
                                        "names": ["O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"],
                                        "struct": {
                                          "types": [{
                                            "i64": {
                                              "typeVariationReference": 0,
                                              "nullability": "NULLABILITY_REQUIRED"
                                            }
                                          }, {
                                            "i64": {
                                              "typeVariationReference": 0,
                                              "nullability": "NULLABILITY_REQUIRED"
                                            }
                                          }, {
                                            "fixedChar": {
                                              "length": 1,
                                              "typeVariationReference": 0,
                                              "nullability": "NULLABILITY_NULLABLE"
                                            }
                                          }, {
                                            "decimal": {
                                              "scale": 0,
                                              "precision": 19,
                                              "typeVariationReference": 0,
                                              "nullability": "NULLABILITY_NULLABLE"
                                            }
                                          }, {
                                            "date": {
                                              "typeVariationReference": 0,
                                              "nullability": "NULLABILITY_NULLABLE"
                                            }
                                          }, {
                                            "fixedChar": {
                                              "length": 15,
                                              "typeVariationReference": 0,
                                              "nullability": "NULLABILITY_NULLABLE"
                                            }
                                          }, {
                                            "fixedChar": {
                                              "length": 15,
                                              "typeVariationReference": 0,
                                              "nullability": "NULLABILITY_NULLABLE"
                                            }
                                          }, {
                                            "i32": {
                                              "typeVariationReference": 0,
                                              "nullability": "NULLABILITY_NULLABLE"
                                            }
                                          }, {
                                            "varchar": {
                                              "length": 79,
                                              "typeVariationReference": 0,
                                              "nullability": "NULLABILITY_NULLABLE"
                                            }
                                          }],
                                          "typeVariationReference": 0,
                                          "nullability": "NULLABILITY_REQUIRED"
                                        }
                                      },
                                         "local_files": {
                                             "items": [
                                             {
                                                 "uri_file": "file://FILENAME_PLACEHOLDER_0",
                                                 "parquet": {}
                                             }
                                             ]
                                         }
                                    }
                                  },
                                  "condition": {
                                    "scalarFunction": {
                                      "functionReference": 0,
                                      "args": [],
                                      "outputType": {
                                        "bool": {
                                          "typeVariationReference": 0,
                                          "nullability": "NULLABILITY_NULLABLE"
                                        }
                                      },
                                      "arguments": [{
                                        "value": {
                                          "scalarFunction": {
                                            "functionReference": 1,
                                            "args": [],
                                            "outputType": {
                                              "bool": {
                                                "typeVariationReference": 0,
                                                "nullability": "NULLABILITY_NULLABLE"
                                              }
                                            },
                                            "arguments": [{
                                              "value": {
                                                "selection": {
                                                  "directReference": {
                                                    "structField": {
                                                      "field": 4
                                                    }
                                                  },
                                                  "rootReference": {
                                                  }
                                                }
                                              }
                                            }, {
                                              "value": {
                                                "cast": {
                                                  "type": {
                                                    "date": {
                                                      "typeVariationReference": 0,
                                                      "nullability": "NULLABILITY_REQUIRED"
                                                    }
                                                  },
                                                  "input": {
                                                    "literal": {
                                                      "fixedChar": "1993-07-01",
                                                      "nullable": false,
                                                      "typeVariationReference": 0
                                                    }
                                                  },
                                                  "failureBehavior": "FAILURE_BEHAVIOR_UNSPECIFIED"
                                                }
                                              }
                                            }]
                                          }
                                        }
                                      }, {
                                        "value": {
                                          "scalarFunction": {
                                            "functionReference": 2,
                                            "args": [],
                                            "outputType": {
                                              "bool": {
                                                "typeVariationReference": 0,
                                                "nullability": "NULLABILITY_NULLABLE"
                                              }
                                            },
                                            "arguments": [{
                                              "value": {
                                                "selection": {
                                                  "directReference": {
                                                    "structField": {
                                                      "field": 4
                                                    }
                                                  },
                                                  "rootReference": {
                                                  }
                                                }
                                              }
                                            }, {
                                              "value": {
                                                "cast": {
                                                  "type": {
                                                    "date": {
                                                      "typeVariationReference": 0,
                                                      "nullability": "NULLABILITY_REQUIRED"
                                                    }
                                                  },
                                                  "input": {
                                                    "literal": {
                                                      "fixedChar": "1993-10-01",
                                                      "nullable": false,
                                                      "typeVariationReference": 0
                                                    }
                                                  },
                                                  "failureBehavior": "FAILURE_BEHAVIOR_UNSPECIFIED"
                                                }
                                              }
                                            }]
                                          }
                                        }
                                      }, {
                                        "value": {
                                          "subquery": {
                                            "setPredicate": {
                                              "predicateOp": "PREDICATE_OP_EXISTS",
                                              "tuples": {
                                                "filter": {
                                                  "common": {
                                                    "direct": {
                                                    }
                                                  },
                                                  "input": {
                                                    "read": {
                                                      "common": {
                                                        "direct": {
                                                        }
                                                      },
                                                      "baseSchema": {
                                                        "names": ["L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"],
                                                        "struct": {
                                                          "types": [{
                                                            "i64": {
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_REQUIRED"
                                                            }
                                                          }, {
                                                            "i64": {
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_REQUIRED"
                                                            }
                                                          }, {
                                                            "i64": {
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_REQUIRED"
                                                            }
                                                          }, {
                                                            "i32": {
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }, {
                                                            "decimal": {
                                                              "scale": 0,
                                                              "precision": 19,
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }, {
                                                            "decimal": {
                                                              "scale": 0,
                                                              "precision": 19,
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }, {
                                                            "decimal": {
                                                              "scale": 0,
                                                              "precision": 19,
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }, {
                                                            "decimal": {
                                                              "scale": 0,
                                                              "precision": 19,
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }, {
                                                            "fixedChar": {
                                                              "length": 1,
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }, {
                                                            "fixedChar": {
                                                              "length": 1,
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }, {
                                                            "date": {
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }, {
                                                            "date": {
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }, {
                                                            "date": {
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }, {
                                                            "fixedChar": {
                                                              "length": 25,
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }, {
                                                            "fixedChar": {
                                                              "length": 10,
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }, {
                                                            "varchar": {
                                                              "length": 44,
                                                              "typeVariationReference": 0,
                                                              "nullability": "NULLABILITY_NULLABLE"
                                                            }
                                                          }],
                                                          "typeVariationReference": 0,
                                                          "nullability": "NULLABILITY_REQUIRED"
                                                        }
                                                      },
                                                     "local_files": {
                                                         "items": [
                                                         {
                                                             "uri_file": "file://FILENAME_PLACEHOLDER_1",
                                                             "parquet": {}
                                                         }
                                                         ]
                                                     }
                                                    }
                                                  },
                                                  "condition": {
                                                    "scalarFunction": {
                                                      "functionReference": 0,
                                                      "args": [],
                                                      "outputType": {
                                                        "bool": {
                                                          "typeVariationReference": 0,
                                                          "nullability": "NULLABILITY_NULLABLE"
                                                        }
                                                      },
                                                      "arguments": [{
                                                        "value": {
                                                          "scalarFunction": {
                                                            "functionReference": 3,
                                                            "args": [],
                                                            "outputType": {
                                                              "bool": {
                                                                "typeVariationReference": 0,
                                                                "nullability": "NULLABILITY_REQUIRED"
                                                              }
                                                            },
                                                            "arguments": [{
                                                              "value": {
                                                                "selection": {
                                                                  "directReference": {
                                                                    "structField": {
                                                                      "field": 0
                                                                    }
                                                                  },
                                                                  "rootReference": {
                                                                  }
                                                                }
                                                              }
                                                            }, {
                                                              "value": {
                                                                "selection": {
                                                                  "directReference": {
                                                                    "structField": {
                                                                      "field": 0
                                                                    }
                                                                  },
                                                                  "outerReference": {
                                                                    "stepsOut": 1
                                                                  }
                                                                }
                                                              }
                                                            }]
                                                          }
                                                        }
                                                      }, {
                                                        "value": {
                                                          "scalarFunction": {
                                                            "functionReference": 2,
                                                            "args": [],
                                                            "outputType": {
                                                              "bool": {
                                                                "typeVariationReference": 0,
                                                                "nullability": "NULLABILITY_NULLABLE"
                                                              }
                                                            },
                                                            "arguments": [{
                                                              "value": {
                                                                "selection": {
                                                                  "directReference": {
                                                                    "structField": {
                                                                      "field": 11
                                                                    }
                                                                  },
                                                                  "rootReference": {
                                                                  }
                                                                }
                                                              }
                                                            }, {
                                                              "value": {
                                                                "selection": {
                                                                  "directReference": {
                                                                    "structField": {
                                                                      "field": 12
                                                                    }
                                                                  },
                                                                  "rootReference": {
                                                                  }
                                                                }
                                                              }
                                                            }]
                                                          }
                                                        }
                                                      }]
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }]
                                    }
                                  }
                                }
                              },
                              "expressions": [{
                                "selection": {
                                  "directReference": {
                                    "structField": {
                                      "field": 5
                                    }
                                  },
                                  "rootReference": {
                                  }
                                }
                              }]
                            }
                          },
                          "groupings": [{
                            "groupingExpressions": [{
                              "selection": {
                                "directReference": {
                                  "structField": {
                                    "field": 0
                                  }
                                },
                                "rootReference": {
                                }
                              }
                            }]
                          }],
                          "measures": [{
                            "measure": {
                              "functionReference": 4,
                              "args": [],
                              "sorts": [],
                              "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
                              "outputType": {
                                "i64": {
                                  "typeVariationReference": 0,
                                  "nullability": "NULLABILITY_REQUIRED"
                                }
                              },
                              "invocation": "AGGREGATION_INVOCATION_ALL",
                              "arguments": []
                            }
                          }]
                        }
                      },
                      "sorts": [{
                        "expr": {
                          "selection": {
                            "directReference": {
                              "structField": {
                                "field": 0
                              }
                            },
                            "rootReference": {
                            }
                          }
                        },
                        "direction": "SORT_DIRECTION_ASC_NULLS_LAST"
                      }]
                    }
                  },
                  "names": ["O_ORDERPRIORITY", "ORDER_COUNT"]
                }
              }],
              "expectedTypeUrls": []
            }
            """,
    }
]