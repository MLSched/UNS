package predictor

import (
	"github.com/MLSched/UNS/pb_gen"
	"github.com/MLSched/UNS/pb_gen/configs"
	"github.com/MLSched/UNS/predictor/dlt_predictor"
	"github.com/MLSched/UNS/predictor/interfaces"
)

func BuildPredictor(configuration *configs.PredictorConfiguration) interfaces.Predictor {
	predictorType := configuration.GetPredictorType()
	switch predictorType {
	case configs.PredictorType_predictorTypeDLTRandom:
		return dlt_predictor.NewRandomPredictor(pb_gen.GetRandomPredictorConfiguration(configuration))
	case configs.PredictorType_predictorTypeDLTDataOriented:
		return dlt_predictor.NewDataOrientedPredictor(pb_gen.GetDataOrientedPredictorConfiguration(configuration))
	default:
		panic("github.com/MLSched/UNSupported predictor type.")
	}
}
