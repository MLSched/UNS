package predictor

import (
	"UNS/pb_gen"
	"UNS/pb_gen/configs"
	"UNS/predictor/dlt_predictor"
	"UNS/predictor/interfaces"
)

func BuildPredictor(configuration *configs.PredictorConfiguration) interfaces.Predictor {
	predictorType := configuration.GetPredictorType()
	switch predictorType {
	case configs.PredictorType_predictorTypeDLTRandom:
		return dlt_predictor.NewRandomPredictor(pb_gen.GetRandomPredictorConfiguration(configuration))
	case configs.PredictorType_predictorTypeDLTDataOriented:
		return dlt_predictor.NewDataOrientedPredictor(pb_gen.GetDataOrientedPredictorConfiguration(configuration))
	default:
		panic("Unsupported predictor type.")
	}
}
