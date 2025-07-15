//AbTestApiProcess.jsx

import React, { useState, useEffect } from 'react';
import { fetchBayesian, fetchAllBayesian, fetchAllInOneBayesian } from '../../../api/AbtestApi';

export function AbTestApiProcess({ mode = "single", ...props }) {
console.log("AbTestApiProcess props: ", props);
  const {
    experimentName = "",
    startDate = "",
    endDate = "",
    metric = "",
    category = "",
    children, // 必须是函数
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
      fetchPromise = fetchBayesian({ experimentName, startDate, endDate, metric, category });
    }


    fetchPromise
      .then(setData)
      .catch((err) => setError(err.message || "Error fetching data"))
      .finally(() => setLoading(false));
  }, [experimentName, startDate, endDate, metric, category, mode]);

  // 外部 children 必须为函数
  return typeof children === "function"
    ? children({ loading, error, data })
    : null;
}
