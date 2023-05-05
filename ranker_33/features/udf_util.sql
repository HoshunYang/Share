USE ROLE TRANSFORMER;

/**
  We implement fit_alpha and fit_beta using method of moments.
  See https://statproofbook.github.io/P/beta-mome
 */
CREATE OR REPLACE FUNCTION fit_alpha(mean FLOAT, variance FLOAT)
returns float
language python
runtime_version = '3.8'
handler = 'fit_alpha_py'
as
$$
def fit_alpha_py(mean, variance):
    if mean is None or variance is None:
        return None
    if mean <= 0 or variance <= 0:
        return None

    alpha = mean * ((mean * (1 - mean) / variance) - 1)

    if alpha <= 0:
        return None
    return alpha
$$;

CREATE OR REPLACE FUNCTION fit_beta(mean FLOAT, variance FLOAT)
returns float
language python
runtime_version = '3.8'
handler = 'fit_beta_py'
as
$$
def fit_beta_py(mean, variance):
    if mean is None or variance is None:
        return None
    if mean <= 0 or variance <= 0:
        return None
    beta = (1 - mean) * ((mean * (1 - mean) / variance) - 1)

    if beta <= 0:
        return None
    return beta
$$;