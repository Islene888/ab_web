// 只负责 API fetch + loading/error/data 传给 children
import React, { useState, useEffect } from 'react';
import { fetchBayesian, fetchAllBayesian, fetchAllInOneBayesian } from '../../../api/abtestApi'; // 注意路径大小写统一

export function AbTestApiProcess({ mode = "single", ...props }) {
  const {
    experimentName = "",
    startDate = "",
    endDate = "",
    metric = "",
    category = "",
    children,
  } = props;
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!experimentName || !startDate || !endDate) return;
    if (mode === 'single' && !metric) return;
    if (mode === 'all' && !category) return;

    setLoading(true);
    setError(null);

    let fetchPromise;
    if (mode === "all") {
      fetchPromise = fetchAllBayesian({ experimentName, startDate, endDate, category });
    } else if (mode === "all_in_one") {
      fetchPromise = fetchAllInOneBayesian({ experimentName, startDate, endDate });
    } else {
      fetchPromise = fetchBayesian({ experimentName, startDate, endDate, metric });
    }

    fetchPromise
      .then(setData)
      .catch((err) => setError(err.message || "Error fetching data"))
      .finally(() => setLoading(false));
  }, [experimentName, startDate, endDate, metric, category, mode]);

  return typeof children === "function"
    ? children({ loading, error, data })
    : null;
}
