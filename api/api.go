package api

import (
	"github.com/cinvat/peretum/api/v1alpha1"
	"github.com/gin-gonic/gin"
)

func Register() *gin.Engine {
	r := gin.Default()
	v1alpha1.Register(r)
	return r
}
