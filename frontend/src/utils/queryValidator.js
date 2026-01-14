export const validateQuery = (query) => {
  const errors = [];
  const warnings = [];

  // Check if query is empty
  if (!query || query.trim().length === 0) {
    errors.push('Query cannot be empty');
    return { isValid: false, errors, warnings };
  }

  // Check query length
  if (query.length > 10000) {
    errors.push('Query exceeds maximum length of 10,000 characters');
  }

  const queryLower = query.toLowerCase().trim();

  // Check if query starts with SELECT
  if (!queryLower.startsWith('select')) {
    errors.push('Only SELECT queries are allowed');
  }

  // Check for dangerous keywords
  const dangerousKeywords = [
    'drop', 'delete', 'truncate', 'insert', 'update',
    'alter', 'create', 'exec', 'execute', 'sp_', 'xp_'
  ];

  for (const keyword of dangerousKeywords) {
    const regex = new RegExp(`\\b${keyword}\\b`, 'i');
    if (regex.test(query)) {
      errors.push(`Keyword '${keyword}' is not allowed for security reasons`);
    }
  }

  // Warning for queries without WHERE clause on Fact tables
  if ((queryLower.includes('fact_flightperformance') || queryLower.includes('fact_delays')) &&
      !queryLower.includes('where') &&
      !queryLower.includes('top')) {
    warnings.push('Consider adding a WHERE clause or TOP N to limit results on large fact tables');
  }

  return {
    isValid: errors.length === 0,
    errors,
    warnings
  };
};

export const getQueryHistory = () => {
  try {
    const history = localStorage.getItem('queryHistory');
    return history ? JSON.parse(history) : [];
  } catch (error) {
    console.error('Error reading query history:', error);
    return [];
  }
};

export const saveQueryToHistory = (query) => {
  try {
    const history = getQueryHistory();
    const newEntry = {
      query,
      timestamp: new Date().toISOString(),
    };
    
    // Add to beginning and keep only last 10
    const updatedHistory = [newEntry, ...history].slice(0, 10);
    localStorage.setItem('queryHistory', JSON.stringify(updatedHistory));
  } catch (error) {
    console.error('Error saving query history:', error);
  }
};

export default validateQuery;