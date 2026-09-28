package v1alpha1

import (
	"github.com/cinvat/peretum/api/v1alpha1/controller"
	"github.com/gin-gonic/gin"
)

const APIVersion = "v1alpha1"

func Register(r *gin.Engine) {
	v1alpha1Router := r.Group(APIVersion)
	// v1alpha1Router.GET("/")

	v1alpha1Router.GET("/targets", controller.ListTargets)
	v1alpha1Router.POST("/targets", controller.CreateTarget)
	v1alpha1Router.PUT("/targets/:server_name", controller.UpdateTarget)
	v1alpha1Router.PATCH("/targets/:server_name", controller.PatchTarget)
	v1alpha1Router.DELETE("/targets/:server_name", controller.DeleteTarget)

}
