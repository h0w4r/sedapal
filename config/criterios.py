"""Definiciones de criterios de criticidad para el análisis de microzonas."""

from __future__ import annotations

from dataclasses import dataclass, field

@dataclass(frozen=True)
class CriteriosCriticidad:
    """Agrupa pesos y umbrales usados para el cálculo del índice crítico.

    Esta clase asegura que los valores numéricos principales se mantengan en rangos razonables.
    """

    peso_ratio: float = field(default=0.6)
    peso_conexiones: float = field(default=0.4)
    percentil_conexiones_critico: float = field(default=15162.0)
    umbral_categoria_alerta: float = field(default=0.3)
    umbral_categoria_critica: float = field(default=0.6)

    def __post_init__(self) -> None:
        """Normaliza y valida pesos y umbrales tras la inicialización del dataclass (estructura de datos ligera)."""
        pesos_validados = max(self.peso_ratio, 0.0) + max(self.peso_conexiones, 0.0)
        if pesos_validados == 0:
            object.__setattr__(self, "peso_ratio", 0.5)
            object.__setattr__(self, "peso_conexiones", 0.5)
        else:
            object.__setattr__(self, "peso_ratio", max(self.peso_ratio, 0.0) / pesos_validados)
            object.__setattr__(self, "peso_conexiones", max(self.peso_conexiones, 0.0) / pesos_validados)

        object.__setattr__(
            self,
            "percentil_conexiones_critico",
            max(float(self.percentil_conexiones_critico), 1.0),
        )

        alerta_normalizada = max(min(float(self.umbral_categoria_alerta), 1.0), 0.0)
        critica_normalizada = max(min(float(self.umbral_categoria_critica), 1.0), 0.0)
        if alerta_normalizada > critica_normalizada:
            alerta_normalizada, critica_normalizada = critica_normalizada, alerta_normalizada

        object.__setattr__(self, "umbral_categoria_alerta", alerta_normalizada)
        object.__setattr__(self, "umbral_categoria_critica", critica_normalizada)

def criterios_por_defecto() -> CriteriosCriticidad:
    """Entrega criterios estándar alineados al percentil 75 de conexiones y pesos definidos en el plan."""
    return CriteriosCriticidad()
