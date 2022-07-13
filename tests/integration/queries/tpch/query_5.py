TESTCASE = [
    {
        "test_name": "test_tpch_sql_5",
        "file_names": ["customer_1.parquet", "orders_1.parquet", "lineitem_1.parquet",
                       "supplier_1.parquet", "nation_1.parquet", "region_1.parquet"],
        "sql_query":
            """
            SELECT
                n_name,
                sum(l_extendedprice * (1 - l_discount)) AS revenue
            FROM
                '{}', '{}', '{}', '{}', '{}', '{}'
            WHERE
                c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND l_suppkey = s_suppkey
                AND c_nationkey = s_nationkey
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'ASIA'
                AND o_orderdate >= CAST('1994-01-01' AS date)
                AND o_orderdate < CAST('1995-01-01' AS date)
            GROUP BY
                n_name
            ORDER BY
                revenue DESC
            """,
        "substrait_query":
            """
            {
              "extensionUris": [{
                "extensionUriAnchor": 1,
                "uri": "/functions_boolean.yaml"
              }, {
                "extensionUriAnchor": 4,
                "uri": "/functions_arithmetic_decimal.yaml"
              }, {
                "extensionUriAnchor": 3,
                "uri": "/functions_datetime.yaml"
              }, {
                "extensionUriAnchor": 2,
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
                  "name": "equal:any1_any1"
                }
              }, {
                "extensionFunction": {
                  "extensionUriReference": 3,
                  "functionAnchor": 2,
                  "name": "gte:date_date"
                }
              }, {
                "extensionFunction": {
                  "extensionUriReference": 3,
                  "functionAnchor": 3,
                  "name": "lt:date_date"
                }
              }, {
                "extensionFunction": {
                  "extensionUriReference": 4,
                  "functionAnchor": 4,
                  "name": "multiply:opt_decimal_decimal"
                }
              }, {
                "extensionFunction": {
                  "extensionUriReference": 4,
                  "functionAnchor": 5,
                  "name": "subtract:opt_decimal_decimal"
                }
              }, {
                "extensionFunction": {
                  "extensionUriReference": 4,
                  "functionAnchor": 6,
                  "name": "sum:opt_decimal"
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
                                  "outputMapping": [47, 48]
                                }
                              },
                              "input": {
                                "filter": {
                                  "common": {
                                    "direct": {
                                    }
                                  },
                                  "input": {
                                    "join": {
                                      "common": {
                                        "direct": {
                                        }
                                      },
                                      "left": {
                                        "join": {
                                          "common": {
                                            "direct": {
                                            }
                                          },
                                          "left": {
                                            "join": {
                                              "common": {
                                                "direct": {
                                                }
                                              },
                                              "left": {
                                                "join": {
                                                  "common": {
                                                    "direct": {
                                                    }
                                                  },
                                                  "left": {
                                                    "join": {
                                                      "common": {
                                                        "direct": {
                                                        }
                                                      },
                                                      "left": {
                                                        "read": {
                                                          "common": {
                                                            "direct": {
                                                            }
                                                          },
                                                          "baseSchema": {
                                                            "names": ["C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"],
                                                            "struct": {
                                                              "types": [{
                                                                "i64": {
                                                                  "typeVariationReference": 0,
                                                                  "nullability": "NULLABILITY_REQUIRED"
                                                                }
                                                              }, {
                                                                "varchar": {
                                                                  "length": 25,
                                                                  "typeVariationReference": 0,
                                                                  "nullability": "NULLABILITY_NULLABLE"
                                                                }
                                                              }, {
                                                                "varchar": {
                                                                  "length": 40,
                                                                  "typeVariationReference": 0,
                                                                  "nullability": "NULLABILITY_NULLABLE"
                                                                }
                                                              }, {
                                                                "i64": {
                                                                  "typeVariationReference": 0,
                                                                  "nullability": "NULLABILITY_REQUIRED"
                                                                }
                                                              }, {
                                                                "fixedChar": {
                                                                  "length": 15,
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
                                                                  "length": 10,
                                                                  "typeVariationReference": 0,
                                                                  "nullability": "NULLABILITY_NULLABLE"
                                                                }
                                                              }, {
                                                                "varchar": {
                                                                  "length": 117,
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
                                                      "right": {
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
                                                                 "uri_file": "file://FILENAME_PLACEHOLDER_1",
                                                                 "parquet": {}
                                                             }
                                                             ]
                                                         }
                                                        }
                                                      },
                                                      "expression": {
                                                        "literal": {
                                                          "boolean": true,
                                                          "nullable": false,
                                                          "typeVariationReference": 0
                                                        }
                                                      },
                                                      "type": "JOIN_TYPE_INNER"
                                                    }
                                                  },
                                                  "right": {
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
                                                             "uri_file": "file://FILENAME_PLACEHOLDER_2",
                                                             "parquet": {}
                                                         }
                                                         ]
                                                     }
                                                    }
                                                  },
                                                  "expression": {
                                                    "literal": {
                                                      "boolean": true,
                                                      "nullable": false,
                                                      "typeVariationReference": 0
                                                    }
                                                  },
                                                  "type": "JOIN_TYPE_INNER"
                                                }
                                              },
                                              "right": {
                                                "read": {
                                                  "common": {
                                                    "direct": {
                                                    }
                                                  },
                                                  "baseSchema": {
                                                    "names": ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"],
                                                    "struct": {
                                                      "types": [{
                                                        "i64": {
                                                          "typeVariationReference": 0,
                                                          "nullability": "NULLABILITY_REQUIRED"
                                                        }
                                                      }, {
                                                        "fixedChar": {
                                                          "length": 25,
                                                          "typeVariationReference": 0,
                                                          "nullability": "NULLABILITY_NULLABLE"
                                                        }
                                                      }, {
                                                        "varchar": {
                                                          "length": 40,
                                                          "typeVariationReference": 0,
                                                          "nullability": "NULLABILITY_NULLABLE"
                                                        }
                                                      }, {
                                                        "i64": {
                                                          "typeVariationReference": 0,
                                                          "nullability": "NULLABILITY_REQUIRED"
                                                        }
                                                      }, {
                                                        "fixedChar": {
                                                          "length": 15,
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
                                                        "varchar": {
                                                          "length": 101,
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
                                                             "uri_file": "file://FILENAME_PLACEHOLDER_3",
                                                             "parquet": {}
                                                         }
                                                         ]
                                                     }
                                                }
                                              },
                                              "expression": {
                                                "literal": {
                                                  "boolean": true,
                                                  "nullable": false,
                                                  "typeVariationReference": 0
                                                }
                                              },
                                              "type": "JOIN_TYPE_INNER"
                                            }
                                          },
                                          "right": {
                                            "read": {
                                              "common": {
                                                "direct": {
                                                }
                                              },
                                              "baseSchema": {
                                                "names": ["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"],
                                                "struct": {
                                                  "types": [{
                                                    "i64": {
                                                      "typeVariationReference": 0,
                                                      "nullability": "NULLABILITY_REQUIRED"
                                                    }
                                                  }, {
                                                    "fixedChar": {
                                                      "length": 25,
                                                      "typeVariationReference": 0,
                                                      "nullability": "NULLABILITY_NULLABLE"
                                                    }
                                                  }, {
                                                    "i64": {
                                                      "typeVariationReference": 0,
                                                      "nullability": "NULLABILITY_REQUIRED"
                                                    }
                                                  }, {
                                                    "varchar": {
                                                      "length": 152,
                                                      "typeVariationReference": 0,
                                                      "nullability": "NULLABILITY_NULLABLE"
                                                    }
                                                  }],
                                                  "typeVariationReference": 0,
                                                  "nullability": "NULLABILITY_REQUIRED"
                                                }
                                              },
                                              "namedTable": {
                                                "names": ["NATION"]
                                              }
                                            }
                                          },
                                          "expression": {
                                            "literal": {
                                              "boolean": true,
                                              "nullable": false,
                                              "typeVariationReference": 0
                                            }
                                          },
                                          "type": "JOIN_TYPE_INNER"
                                        }
                                      },
                                      "right": {
                                        "read": {
                                          "common": {
                                            "direct": {
                                            }
                                          },
                                          "baseSchema": {
                                            "names": ["R_REGIONKEY", "R_NAME", "R_COMMENT"],
                                            "struct": {
                                              "types": [{
                                                "i64": {
                                                  "typeVariationReference": 0,
                                                  "nullability": "NULLABILITY_REQUIRED"
                                                }
                                              }, {
                                                "fixedChar": {
                                                  "length": 25,
                                                  "typeVariationReference": 0,
                                                  "nullability": "NULLABILITY_NULLABLE"
                                                }
                                              }, {
                                                "varchar": {
                                                  "length": 152,
                                                  "typeVariationReference": 0,
                                                  "nullability": "NULLABILITY_NULLABLE"
                                                }
                                              }],
                                              "typeVariationReference": 0,
                                              "nullability": "NULLABILITY_REQUIRED"
                                            }
                                          },
                                          "namedTable": {
                                            "names": ["REGION"]
                                          }
                                        }
                                      },
                                      "expression": {
                                        "literal": {
                                          "boolean": true,
                                          "nullable": false,
                                          "typeVariationReference": 0
                                        }
                                      },
                                      "type": "JOIN_TYPE_INNER"
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
                                                      "field": 9
                                                    }
                                                  },
                                                  "rootReference": {
                                                  }
                                                }
                                              }
                                            }]
                                          }
                                        }
                                      }, {
                                        "value": {
                                          "scalarFunction": {
                                            "functionReference": 1,
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
                                                      "field": 17
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
                                                      "field": 8
                                                    }
                                                  },
                                                  "rootReference": {
                                                  }
                                                }
                                              }
                                            }]
                                          }
                                        }
                                      }, {
                                        "value": {
                                          "scalarFunction": {
                                            "functionReference": 1,
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
                                                      "field": 19
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
                                                      "field": 33
                                                    }
                                                  },
                                                  "rootReference": {
                                                  }
                                                }
                                              }
                                            }]
                                          }
                                        }
                                      }, {
                                        "value": {
                                          "scalarFunction": {
                                            "functionReference": 1,
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
                                                      "field": 3
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
                                                      "field": 36
                                                    }
                                                  },
                                                  "rootReference": {
                                                  }
                                                }
                                              }
                                            }]
                                          }
                                        }
                                      }, {
                                        "value": {
                                          "scalarFunction": {
                                            "functionReference": 1,
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
                                                      "field": 36
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
                                                      "field": 40
                                                    }
                                                  },
                                                  "rootReference": {
                                                  }
                                                }
                                              }
                                            }]
                                          }
                                        }
                                      }, {
                                        "value": {
                                          "scalarFunction": {
                                            "functionReference": 1,
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
                                                      "field": 42
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
                                                      "field": 44
                                                    }
                                                  },
                                                  "rootReference": {
                                                  }
                                                }
                                              }
                                            }]
                                          }
                                        }
                                      }, {
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
                                                      "field": 45
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
                                                    "fixedChar": {
                                                      "length": 25,
                                                      "typeVariationReference": 0,
                                                      "nullability": "NULLABILITY_REQUIRED"
                                                    }
                                                  },
                                                  "input": {
                                                    "literal": {
                                                      "fixedChar": "ASIA",
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
                                                      "field": 12
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
                                                      "fixedChar": "1994-01-01",
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
                                            "functionReference": 3,
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
                                                      "field": 12
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
                                                      "fixedChar": "1995-01-01",
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
                                      }]
                                    }
                                  }
                                }
                              },
                              "expressions": [{
                                "selection": {
                                  "directReference": {
                                    "structField": {
                                      "field": 41
                                    }
                                  },
                                  "rootReference": {
                                  }
                                }
                              }, {
                                "scalarFunction": {
                                  "functionReference": 4,
                                  "args": [],
                                  "outputType": {
                                    "decimal": {
                                      "scale": 0,
                                      "precision": 19,
                                      "typeVariationReference": 0,
                                      "nullability": "NULLABILITY_NULLABLE"
                                    }
                                  },
                                  "arguments": [{
                                    "value": {
                                      "selection": {
                                        "directReference": {
                                          "structField": {
                                            "field": 22
                                          }
                                        },
                                        "rootReference": {
                                        }
                                      }
                                    }
                                  }, {
                                    "value": {
                                      "scalarFunction": {
                                        "functionReference": 5,
                                        "args": [],
                                        "outputType": {
                                          "decimal": {
                                            "scale": 0,
                                            "precision": 19,
                                            "typeVariationReference": 0,
                                            "nullability": "NULLABILITY_NULLABLE"
                                          }
                                        },
                                        "arguments": [{
                                          "value": {
                                            "cast": {
                                              "type": {
                                                "decimal": {
                                                  "scale": 0,
                                                  "precision": 19,
                                                  "typeVariationReference": 0,
                                                  "nullability": "NULLABILITY_NULLABLE"
                                                }
                                              },
                                              "input": {
                                                "literal": {
                                                  "i32": 1,
                                                  "nullable": false,
                                                  "typeVariationReference": 0
                                                }
                                              },
                                              "failureBehavior": "FAILURE_BEHAVIOR_UNSPECIFIED"
                                            }
                                          }
                                        }, {
                                          "value": {
                                            "selection": {
                                              "directReference": {
                                                "structField": {
                                                  "field": 23
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
                              "functionReference": 6,
                              "args": [],
                              "sorts": [],
                              "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
                              "outputType": {
                                "decimal": {
                                  "scale": 0,
                                  "precision": 19,
                                  "typeVariationReference": 0,
                                  "nullability": "NULLABILITY_NULLABLE"
                                }
                              },
                              "invocation": "AGGREGATION_INVOCATION_ALL",
                              "arguments": [{
                                "value": {
                                  "selection": {
                                    "directReference": {
                                      "structField": {
                                        "field": 1
                                      }
                                    },
                                    "rootReference": {
                                    }
                                  }
                                }
                              }]
                            }
                          }]
                        }
                      },
                      "sorts": [{
                        "expr": {
                          "selection": {
                            "directReference": {
                              "structField": {
                                "field": 1
                              }
                            },
                            "rootReference": {
                            }
                          }
                        },
                        "direction": "SORT_DIRECTION_DESC_NULLS_FIRST"
                      }]
                    }
                  },
                  "names": ["N_NAME", "REVENUE"]
                }
              }],
              "expectedTypeUrls": []
            }
            """,
    }
]