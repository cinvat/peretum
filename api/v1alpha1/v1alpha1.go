package v1alpha1

import (
	"github.com/gin-gonic/gin"
)

const APIVersion = "v1alpha1"

func Register(r *gin.Engine) {
	v1alpha1Router := r.Group(APIVersion)
	v1alpha1Router.GET("/")
}
