'use client';

import { useState } from 'react';

export default function Home() {
  const [daxQuery, setDaxQuery] = useState(`EVALUATE VALUES(BU)`);
  const [queryResult, setQueryResult] = useState<any[] | null>(null);
  const [queryError, setQueryError] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [datasetName, setDatasetName] = useState('Employee Hiring and History');

  const handleRunQuery = async () => {
    if (!daxQuery) {
      alert("Please build a DAX query first.");
      return;
    }

    setIsLoading(true);
    setQueryError('');
    setQueryResult(null);

    try {
      const response = await fetch('http://localhost:5032/api/powerbi/run-query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          daxQuery,
          catalog: datasetName,
        }),
      });

      const contentType = response.headers.get('content-type');

      if (!response.ok) {
        const errorText = await response.text();
        setQueryError(`Error: ${errorText}`);
        return;
      }

      if (contentType?.includes('application/json')) {
        const data = await response.json();
        setQueryResult(data);
      } else {
        const text = await response.text();
        setQueryError(`Non-JSON response: ${text}`);
      }
    } catch (error: any) {
      setQueryError(`Error: ${error.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const renderTable = () => {
    if (!queryResult || queryResult.length === 0) return <p>No results returned.</p>;

    const headers = Object.keys(queryResult[0]);

    return (
      <div className="overflow-auto border border-gray-300 rounded">
        <table className="min-w-full table-auto text-sm">
          <thead>
            <tr className="bg-gray-200">
              {headers.map((header) => (
                <th key={header} className="px-3 py-2 border-b text-left font-semibold">
                  {header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {queryResult.map((row, i) => (
              <tr key={i} className="even:bg-gray-50">
                {headers.map((header) => (
                  <td key={header} className="px-3 py-2 border-b">
                    {row[header]?.toString() ?? ''}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };

  return (
    <main className="p-6 font-sans">
      <h1 className="text-2xl font-bold mb-4">Run DAX Query</h1>

      <div className="mb-4">
        <label className="block font-semibold mb-2">Dataset Name</label>
        <input
          type="text"
          value={datasetName}
          onChange={(e) => setDatasetName(e.target.value)}
          className="w-full p-2 border border-gray-300 rounded mb-4"
        />

        <label className="block font-semibold mb-2">DAX Query</label>
        <textarea
          value={daxQuery}
          onChange={(e) => setDaxQuery(e.target.value)}
          rows={6}
          className="w-full p-2 border border-gray-300 rounded"
          placeholder="Enter your DAX query here..."
        />
      </div>

      <button
        onClick={handleRunQuery}
        disabled={isLoading}
        className={`${
          isLoading ? 'bg-blue-400' : 'bg-blue-600 hover:bg-blue-700'
        } text-white px-4 py-2 rounded transition`}
      >
        {isLoading ? 'Running...' : 'Run Query'}
      </button>

      <div className="mt-6">
        <h2 className="font-semibold mb-2">Query Output</h2>

        {queryError && (
          <div className="text-red-600 bg-red-50 border border-red-300 p-3 rounded mb-4">
            {queryError}
          </div>
        )}

        {queryResult && renderTable()}
      </div>
    </main>
  );
}
