"""Contracts for the retired unified-capacity deployment evidence."""

import pathlib
import re

_DESIGN_PATH = (pathlib.Path(__file__).resolve().parents[2] / 'docs' /
                'designs' / 'unified-physical-capacity-convergence.md')
_APPLICATION_LAYER_DIFF_IDS = {
    'sha256:fecd35d37becf86a13127f7aed2d017e12c3c3fa62f6db5f52c5cd9b08c7950b',
    'sha256:be39f8e77ffdccbd68c9c34f32810772ad213bafad678310d8e697d821453ffd',
}


def test_capacity_image_application_layer_evidence() -> None:
    design = _DESIGN_PATH.read_text(encoding='utf-8')
    match = re.search(
        r'The registry vulnerability report.*?This is causal attribution',
        design,
        flags=re.DOTALL,
    )

    assert match is not None
    documented_layers = set(re.findall(r'sha256:[0-9a-f]{64}', match.group(0)))
    assert documented_layers == _APPLICATION_LAYER_DIFF_IDS
